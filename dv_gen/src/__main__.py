from pathlib import Path
from data_vault_generator import DataVaultGenerator
import subprocess
from loguru import logger

def run_dbt_macro_gen_sources(dv_source_path: Path):
    result = subprocess.run(
            ["dbt", "--quiet", "run-operation", "dv_generate_source"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
    
    if result.returncode:
        logger.error(result.stdout)
        return
        
    with open(dv_source_path, "w", encoding='utf-8') as f:
        f.write(result.stdout)
    
    logger.info('sources generated')
    
        
    
 

def main():
    stg_dir = Path(__file__).parent.parent.parent / 'tsm_dwh' / 'models' /  'stage'
    dv_dir = Path(__file__).parent.parent.parent / 'tsm_dwh'/ 'models' / 'dv'
    dv_source_path = dv_dir / 'sources.yml'
    
    run_dbt_macro_gen_sources(dv_source_path)
    
    DataVaultGenerator(stg_dir, dv_dir).run()
    logger.info('data vault generated')
    
    

if __name__ == '__main__':
    
    main()