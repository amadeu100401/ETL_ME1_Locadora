import re
from traceback import print_tb
from unittest import result
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import delete
from sqlalchemy import text
import sqlalchemy as sa
from time import perf_counter
import time
import timeit
from Entidades import artista,copias,dmArtistas,dmGravadora,dmSocio,dmTempo,dmTitulo,ftLocacoes,gravadoras,itensLocacao,locacoes,socios,tipoSocio,titulos



BASE = declarative_base()

def connect_db():
  print("Abrindo conexão com o banco!")
  DIALECT = 'oracle'
  SQL_DRIVER = 'cx_oracle'
  USERNAME = 'adm' #enter your username
  PASSWORD = '123456789' #enter your password
  HOST = 'oracle-74471-0.cloudclusters.net' #enter the oracle db host url
  PORT = 19698 # enter the oracle port number
  SERVICE = 'XE' # enter the oracle db service name
  ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

  engine = sa.create_engine(ENGINE_PATH_WIN_AUTH)
  print("Conexão aberta!")
  return engine
  
engine = connect_db()
# print(engine.table_names())
# engine.connect()
metadata = sa.MetaData(bind=None)

#Tabelas
table_artistas = sa.Table('ARTISTAS', metadata, autoload=True, autoload_with=engine)
table_copias = sa.Table('COPIAS', metadata, autoload=True, autoload_with=engine)
table_gravadoras = sa.Table('GRAVADORAS', metadata, autoload=True, autoload_with=engine)
table_itensLocacoes = sa.Table('ITENS_LOCACOES', metadata, autoload=True, autoload_with=engine)
table_locacoes = sa.Table('LOCACOES', metadata, autoload=True, autoload_with=engine)
table_socios = sa.Table('SOCIOS', metadata, autoload=True, autoload_with=engine)
table_tipoSocios = sa.Table('TIPOS_SOCIOS', metadata, autoload=True, autoload_with=engine)
table_titulos = sa.Table('TITULOS', metadata, autoload=True, autoload_with=engine)

dm_artista = sa.Table('DM_ARTISTA', metadata, autoload=True, autoload_with=engine)
dm_gravadora = sa.Table('DM_GRAVADORA', metadata, autoload=True, autoload_with=engine)
dm_socio = sa.Table('DM_SOCIO', metadata, autoload=True, autoload_with=engine)
dm_tempo = sa.Table('DM_TEMPO', metadata, autoload=True, autoload_with=engine)
dm_titulo = sa.Table('DM_TITULO', metadata, autoload=True, autoload_with=engine)
ft_locacoes = sa.Table('FT_LOCACOES', metadata, autoload=True, autoload_with=engine)

def LimparBase():
    start = perf_counter()  
    print("Iniciando limpeza da base de dados!")
    #stmt = text('TRUNCATE TABLE ft_locacoes')
    engine.execute(text('DELETE ft_locacoes'))
    engine.execute(text('DELETE dm_artista'))
    engine.execute(text('DELETE dm_gravadora'))
    engine.execute(text('DELETE dm_socio'))
    engine.execute(text('DELETE dm_tempo'))
    engine.execute(text('DELETE dm_titulo'))
    end = perf_counter()
    r = (end - start)*100
    print(f"Tempo total da execução: {r} segundos")

def ExtractArtista():
    art1 = []
    count = 0
    print("Iniciando extração de Artista")
    start = timeit.default_timer()

    stmt = sa.select([table_artistas])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        art1.append(artista.Artistas(row[0],row[1],row[2],row[3],row[4],row[5],row[6]))
        count += 1
    
    #for i in art1:
    #   print(i.cod_art)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return art1
        
def ExtrairCopias():
    cop1 = []
    count = 0
    print("Iniciando extração de Copias")
    start = timeit.default_timer()

    stmt = sa.select([table_copias])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        cop1.append(copias.Copias(row[0],row[1],row[2],row[3]))
        count += 1
    
    #for i in cop1:
    #    print(i.cod_tit)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return cop1

def ExtrairGravadoras():
    grav1 = []
    count = 0
    print("Iniciando extração de Gravadoras")
    start = timeit.default_timer()

    stmt = sa.select([table_gravadoras])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        grav1.append(gravadoras.Gravadoras(row[0],row[1],row[2],row[3]))
        count += 1
    
    #for i in grav1:
    #    print(i.nom_grav)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return grav1

def ExtrairItensLocacoes():
    il1 = []
    count = 0
    print("Iniciando extração de Itens locacoes")
    start = timeit.default_timer()

    stmt = sa.select([table_itensLocacoes])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        il1.append(itensLocacao.ItensLocacoes(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
        count += 1
    
    #for i in il1:
    #    print(i.dat_dev)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return il1

def ExtrairLocacoes():
    l1 = []
    count = 0
    print("Iniciando extração de Locacoes")
    start = timeit.default_timer()

    stmt = sa.select([table_locacoes])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        l1.append(locacoes.Locacoes(row[0],row[1],row[2],row[3],row[4],row[5]))
        count += 1
    
    #for i in l1:
    #    print(i.val_loc)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return l1

def ExtrairSocios():
    s1 = []
    count = 0
    print("Iniciando extração de Socios")
    start = timeit.default_timer()

    stmt = sa.select([table_socios])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        s1.append(socios.Socios(row[0],row[1],row[2],row[3],row[4]))
        count += 1
    
    #for i in s1:
    #    print(i.nom_soc)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return s1

def ExtrairTipoSocios():
    ts1 = []
    count = 0
    print("Iniciando extração de Tipo Socios")
    start = timeit.default_timer()

    stmt = sa.select([table_tipoSocios])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        ts1.append(tipoSocio.TipoSocios(row[0],row[1],row[2],row[3]))
        count += 1
    
    for i in ts1:
        print(i.dsc_tps)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return ts1

def ExtrairTitulos():
    t1 = []
    count = 0
    print("Iniciando extração de Titulos")
    start = timeit.default_timer()

    stmt = sa.select([table_titulos])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        t1.append(titulos.Titulos(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
        count += 1
    
    for i in t1:
        print(i.dsc_tit)

    end = timeit.default_timer()
    r = (end - start)
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return t1



    