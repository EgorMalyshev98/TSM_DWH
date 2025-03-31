from datetime import datetime
import json
import jsonschema
from transform.journal_landing import JournalProcessor
from config import MAP_SETTINGS
from transform.data_vault_etl import DataVaultETL
from log import lg


def select_landing_processor(msg_type: str):
    if msg_type == "DocumentRef.ТСМ_ЖУФВР":
        return JournalProcessor
    raise NotImplementedError(f'processor for source {msg_type} not be implemented')


def process_message(msg):
    rmq_message = json.loads(msg)
    try:
        msg_type = rmq_message["type"]
        land_processor = select_landing_processor(msg_type)
        source_tables_dict = land_processor.process(rmq_message)
        etl = DataVaultETL('жуфвр_1с', source_tables_dict)
        etl.run()
    except  jsonschema.ValidationError:
        file_name = datetime.now().strftime('%d_%m_%Y__%H_%M_%S') + 'validation' + '.json'
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(rmq_message, f, ensure_ascii=False, indent=4)
            
        lg.info('broken msg is saved')
        
    except Exception as e:
        file_name = datetime.now().strftime('%d_%m_%Y__%H_%M_%S') + '.json'
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(rmq_message, f, ensure_ascii=False, indent=4)
            
        lg.info('broken msg is saved')
    
        raise e
        
        
    
