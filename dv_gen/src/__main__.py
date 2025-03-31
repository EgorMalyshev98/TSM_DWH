from pathlib import Path
from data_vault_generator import DataVaultGenerator

def main():
    stg_dir = Path(__file__).parent.parent.parent / 'tsm_dwh' / 'models' /  'stage'
    dv_dir = Path(__file__).parent.parent.parent / 'tsm_dwh'/ 'models' /'dv'
    DataVaultGenerator(stg_dir, dv_dir).run()
    

if __name__ == '__main__':
    main()