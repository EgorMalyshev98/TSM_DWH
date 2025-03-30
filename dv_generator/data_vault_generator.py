from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, OrderedDict
from config import DV_METADATA, TEMPLATES
from loguru import logger as lg
import pandas as pd


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


class DataVaultGenerator:
    def __init__(self, 
                 stg_dir,
                 dv_dir,
                 dv_conf = DataVaultConfig, 
                 metadata = DV_METADATA
                 ):
        
        # self.loadts = datetime.now(timezone.utc).isoformat()
        self.metadata = metadata
        self.stg_dir = stg_dir
        self.dv_dir = dv_dir
        self.stg_cols_dict = OrderedDict()
        self.cnf = dv_conf

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

        return sql_template.format_map(format_map)


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

        return sql_template.format_map(format_map)


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
        
        return sql_template.format_map(format_map)

        
    
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
        return sql_template.format_map(format_map)
        

    def process_target_tabels(self, table_name, table_group: pd.DataFrame, stg_table_name: str, src_columns: list):
        target_table_type = table_group["target_table_type"].to_numpy()[0]
        if target_table_type == "hub":
            return self.hub_handle(table_name, table_group, stg_table_name)
        if target_table_type == "link":
            return self.link_handle(table_name, table_group, stg_table_name)
        if target_table_type == "sat":
            return self.sat_handle(table_name, table_group,
                            stg_table_name, src_columns)
        if target_table_type == "msat":
            return self.msat_handle(table_name, table_group,
                            stg_table_name, src_columns)
            

    def generate_sql(self, source_group: pd.DataFrame):
        for stg_table_name, stg_table_group in source_group.groupby("stg_table"):
            src_columns = stg_table_group["source_column"].values
            src_table = stg_table_group["source_table"].to_numpy()[0]
            
            for target_table_name, target_table_group in stg_table_group.groupby("target_table"):
                stmt = self.process_target_tabels(target_table_name,
                                           target_table_group,
                                           stg_table_name,
                                           src_columns)
                
                fp = (self.dv_dir / target_table_name).with_suffix('.sql')
                
                with open(fp, 'w', encoding='utf-8') as f:
                    f.write(stmt)

            stg_col_names = ",\n\t".join(self.stg_cols_dict.keys())
            stg_col_select = ",\n\t".join(self.stg_cols_dict.values())
            
            src_str_columns = ",\n\t".join(src_columns)
            stg_format_map = {
                "hkcode": "default",
                "target_table": stg_table_name,
                "target_columns": stg_col_names,
                "select_cols": stg_col_select,
                "src_columns": src_str_columns,
                "src_table": src_table}
            
            stage_sql_tmplate = TEMPLATES["insert"]["stage"]
            stage_sql = stage_sql_tmplate.format_map(stg_format_map)
            
            fp = (self.stg_dir / stg_table_name).with_suffix('.sql')
            
            with open(fp, 'w', encoding='utf-8') as f:
                f.write(stage_sql)
 
            self.stg_cols_dict = OrderedDict()
            
    def make_dirs(self):
        self.stg_dir.mkdir(exist_ok=True, parents=True)
        self.dv_dir.mkdir(exist_ok=True, parents=True)
        
    def run(self):
        self.make_dirs()
        for source, source_group in self.metadata.groupby('record_source'):
            self.generate_sql(source_group)
            
        
if __name__ == '__main__':
    stg_dir = Path(__file__).parent.parent / 'tsm_dwh' / 'models' /  'stage'
    dv_dir = Path(__file__).parent.parent / 'tsm_dwh'/ 'models' /'dv'
    DataVaultGenerator(stg_dir, dv_dir).run()
    


