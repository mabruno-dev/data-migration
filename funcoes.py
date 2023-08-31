from configparser import ConfigParser as ini

def ler_ini(section:str,option:str) -> str:
    config = ini()
    config.read('config.ini')
    return str(config.get(section,option))

def criar_ini():
    config = ini()

    config.add_section('CONNECTION_SOURCE')
    config['CONNECTION_SOURCE']['HOST'] = 'INFDC-WS-01\TOTALDSV'
    config['CONNECTION_SOURCE']['DATABASE'] = 'onboardold'
    config['CONNECTION_SOURCE']['USERNAME'] = 'Programador_Inffel'
    config['CONNECTION_SOURCE']['PASSWORD'] = 'pr0gr4minff&l'

    config.add_section('CONNECTION_TARGET')
    config['CONNECTION_TARGET']['HOST'] = 'INFDC-WS-01\TOTALDSV'
    config['CONNECTION_TARGET']['DATABASE'] = 'Onboard'
    config['CONNECTION_TARGET']['USERNAME'] = 'Programador_Inffel'
    config['CONNECTION_TARGET']['PASSWORD'] = 'pr0gr4minff&l'

    config.add_section('QUERY_PARAMS')
    config['QUERY_PARAMS']['SOURCE_DATE'] = '2023-03-01'

    with open('config.ini','w') as f:
        config.write(f)
        f.close()

def criar_par_ini(section:str,option:str,default:str):
    try:
        config = ini()
        config.read('config.ini')
        config.get(section,option)
        
    except Exception as E:
        config[section][option] = default
        
        with open('config.ini','w') as f:
            config.write(f)
            f.close()
        
        
    