from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, OrderedDict
from config import MAP_SETTINGS, TEMPLATES
from loguru import 


@dataclass
class DataVaultConfig:
    hk_divider = " || '|' || "
    hkcode = "'default'"
    hk_prefix = "hk_"
    bk_template = "LOWER(TRIM(COALESCE({src_bk}, '-1')::varchar))"
    hk_template = "digest({hkcode}{hk_divider}{bk}, 'sha1') as {hk_name}"
    hdiff_template = "digest({attrs}, 'sha1') as {hdiff_name}"
    attr_hdiff_template = r"TRIM(COALESCE({attr}::varchar, 'N\A'))"
    col_process_template = "{proc_func}({col_name})"


class DataVaultETL:
    cnf = DataVaultConfig

    def __init__(self, source: str, source_tables_dict: Dict[str, pd.DataFrame]):
        self.loadts = datetime.now(timezone.utc).isoformat()
        self.stg_groups = MAP_SETTINGS[MAP_SETTINGS["record_source"] == source].fillna(
            "").groupby("stg_table")
        self.record_source = source
        self.data_tables = source_tables_dict
        self.stg_cols_dict = OrderedDict()
        self.stage_stmt_list = []
        self.hubs_stmt = []
        self.links_stmt = []
        self.sats_stmt = []

    def hub_handle(self, table_name: str, table_group: pd.DataFrame, stg_table_name: str):
        hk_name = self.cnf.hk_prefix + table_name
        bkeys_dict = OrderedDict(
            zip(table_group["stg_column"], table_group['source_column'].map(
                lambda x: self.cnf.bk_template.format(src_bk=x)))
        )
        hk_value = self.cnf.hk_template.format(
            hkcode=self.cnf.hkcode,
            hk_divider=self.cnf.hk_divider,
            bk=self.cnf.hk_divider.join(bkeys_dict.values()),
            hk_name=hk_name
        )
        self.stg_cols_dict.setdefault(hk_name, hk_value)
        {self.stg_cols_dict.setdefault(key, value)
         for key, value in bkeys_dict.items()}

        sql_template = TEMPLATES['insert']['hub']

        format_map = {
            "table_name": table_name,
            "hk_name": hk_name,
            "bk_cols": ",\n\t".join(bkeys_dict.keys()),
            "stg_table": stg_table_name,
        }

        self.hubs_stmt.append(sql_template.format_map(format_map))

    def link_handle(self, table_name: str, table_group: pd.DataFrame, stg_table_name: str):
        hk_name = self.cnf.hk_prefix + table_name
        bkeys_dict = OrderedDict(
            zip(table_group["stg_column"], table_group["source_column"].map(
                lambda x: self.cnf.bk_template.format(src_bk=x)))
        )
        hk_value = self.cnf.hk_template.format(
            hkcode=self.cnf.hkcode,
            hk_divider=self.cnf.hk_divider,
            bk=self.cnf.hk_divider.join(bkeys_dict.values()),
            hk_name=hk_name
        )
        self.stg_cols_dict.setdefault(hk_name, hk_value)
        {self.stg_cols_dict.setdefault(key, value)
         for key, value in bkeys_dict.items()}

        parent_hkeys = []
        for parent_table, parent_table_group in table_group.groupby("parent_table"):
            p_hk_name = self.cnf.hk_prefix + parent_table
            p_bkeys_dict = OrderedDict(
                zip(parent_table_group["stg_column"],
                    parent_table_group["source_column"].map(lambda x: self.cnf.bk_template.format(src_bk=x)))
            )
            p_hk_value = self.cnf.hk_template.format(
                hkcode=self.cnf.hkcode,
                hk_divider=self.cnf.hk_divider,
                bk=self.cnf.hk_divider.join(p_bkeys_dict.values()),
                hk_name=p_hk_name
                )
            self.stg_cols_dict.setdefault(p_hk_name, p_hk_value)

            {self.stg_cols_dict.setdefault(key, value)
             for key, value in p_bkeys_dict.items()}
            parent_hkeys.append(p_hk_name)

        sql_template = TEMPLATES['insert']['link']
        format_map = {
            "table_name": table_name,
            "hk_name": hk_name,
            "parent_hkeys": ",\n\t".join(parent_hkeys),
            "stg_table": stg_table_name
        }

        self.links_stmt.append(sql_template.format_map(format_map))

    def sat_handle(self, table_name: str, table_group: pd.DataFrame, stg_table_name: str, src_columns: list):
        # stage
        attrs_df = table_group[(table_group.target_key_type == "attr") & (table_group.source_column.isin(src_columns))]
        hdiff_name = 'hdiff_' + table_name
        
        hdiff_list = attrs_df["source_column"].map(
            lambda x: self.cnf.attr_hdiff_template.format(attr=x))
        
        hdiff_value = self.cnf.hdiff_template.format(
            attrs=self.cnf.hk_divider.join(hdiff_list),
            hdiff_name=hdiff_name
        )
        attrs_dict = OrderedDict(
            zip(
                attrs_df["stg_column"],
                attrs_df.apply(
                    lambda row: f"{row['source_column']}::{row['col_type']}", axis=1)
            ))

        self.stg_cols_dict.setdefault(hdiff_name, hdiff_value)
        {self.stg_cols_dict.setdefault(key, value) for key, value in attrs_dict.items()}

        # dds
        hk_parent_name = 'hk_' + \
            table_group[table_group.target_key_type == "bk"]['parent_table'].values[0]
            
        stg_cols = ",\n\t".join(attrs_df["stg_column"].to_numpy().tolist())
        format_map = {
            "target_table_name": table_name,
            "hdiff_name": hdiff_name,
            "hk_parent_name": hk_parent_name,
            "stg_table": stg_table_name,
            "stg_cols": stg_cols
        }

        sql_template = TEMPLATES['insert']['sat']
        self.sats_stmt.append(sql_template.format_map(format_map))
        
    
    
    def msat_handle(self, table_name: str, table_group: pd.DataFrame, stg_table_name: str, src_columns: list):
        # stage
        attrs_df = table_group[(table_group.target_key_type == "attr") & (table_group.source_column.isin(src_columns))]
        hdiff_name = 'hdiff_' + table_name
        hdiff_list = attrs_df["source_column"].map(
            lambda x: self.cnf.attr_hdiff_template.format(attr=x))
        hdiff_value = self.cnf.hdiff_template.format(
            attrs=self.cnf.hk_divider.join(hdiff_list),
            hdiff_name=hdiff_name
        )
        attrs_dict = OrderedDict(
            zip(
                attrs_df["stg_column"],
                attrs_df.apply(
                    lambda row: f"{row['source_column']}::{row['col_type']}", axis=1)
            ))

        self.stg_cols_dict.setdefault(hdiff_name, hdiff_value)
        {self.stg_cols_dict.setdefault(key, value)
         for key, value in attrs_dict.items()}

        # dds
        hk_parent_name = 'hk_' + \
            table_group[table_group.target_key_type ==
                        "bk"]['parent_table'].values[0]
        stg_cols = ",\n\t".join(attrs_df["stg_column"].to_numpy().tolist())
        format_map = {
            "target_table_name": table_name,
            "hdiff_name": hdiff_name,
            "hk_parent_name": hk_parent_name,
            "stg_table": stg_table_name,
            "stg_cols": stg_cols
        }

        sql_template = TEMPLATES['insert']['msat']
        self.sats_stmt.append(sql_template.format_map(format_map))
        

    def process_target_tabels(self, table_name, table_group: pd.DataFrame, stg_table_name: str, src_columns: list):
        target_table_type = table_group["target_table_type"].to_numpy()[0]
        if target_table_type == "hub":
            self.hub_handle(table_name, table_group, stg_table_name)
        if target_table_type == "link":
            self.link_handle(table_name, table_group, stg_table_name)
        if target_table_type == "sat":
            self.sat_handle(table_name, table_group,
                            stg_table_name, src_columns)
        if target_table_type == "msat":
            self.msat_handle(table_name, table_group,
                            stg_table_name, src_columns)

    def generate_sql(self):
        for stg_table_name, stg_table_group in self.stg_groups:
            src_table = stg_table_group["source_table"].to_numpy()[0]
            
            if self.data_tables[src_table].empty:
                lg.debug(f'{src_table} is empty, skipping')
                continue
            
            data_cols_set = set(self.data_tables[src_table].columns)
            src_columns = list(data_cols_set.intersection(stg_table_group["source_column"].values))
            
            for target_table_name, target_table_group in stg_table_group.groupby("target_table"):
                self.process_target_tabels(target_table_name,
                                           target_table_group,
                                           stg_table_name,
                                           src_columns)

            stg_col_names = ",\n\t".join(self.stg_cols_dict.keys())
            stg_col_select = ",\n\t".join(self.stg_cols_dict.values())
            
            values = self.data_tables[src_table][src_columns].to_numpy().tolist()
            src_str_columns = ",\n\t".join(src_columns)
            placeholders = ", ".join(["%s"] * len(src_columns))

            format_map = {
                "recsource": self.record_source,
                "loadts": self.loadts,
                "hkcode": "default",
                "target_table": stg_table_name,
                "target_columns": stg_col_names,
                "select_cols": stg_col_select,
                "placeholders": placeholders,
                "src_columns": src_str_columns,
            }
            stage_sql_tmplate = TEMPLATES["insert"]["stage"]
            stmt = {
                'stmt': stage_sql_tmplate.format_map(format_map),
                'values': values
            }
            self.stage_stmt_list.append(stmt)
            self.stg_cols_dict = OrderedDict()


    def insert(self):
        # stage_stmt = "\n\n".join([stmt_dict['stmt']
        #                            for stmt_dict in self.stage_stmt_list])
        hub_stmt = "\n\n".join(self.hubs_stmt)
        link_stmt = "\n\n".join(self.links_stmt)
        sat_stmt = "\n\n".join(self.sats_stmt)

        # with Path("insert_stage.sql").open("w", encoding="utf-8") as f:
        #     f.write(stage_stmt)
        # with Path("insert_hub.sql").open("w", encoding="utf-8") as f:
        #     f.write(hub_stmt)
        # with Path("insert_link.sql").open("w", encoding="utf-8") as f:
        #     f.write(link_stmt)
        

    def run(self):
        self.generate_sql()
        self.insert()


