# -*- coding: utf-8 -*-
"""
Created on Wed Dec 21 14:14:08 2022
@author: Kevin
"""
import pyodbc
import pandas as pd
import cx_Oracle as orcCon
from cx_Oracle import DatabaseError
from datetime import datetime 

"""
Limpia espacios en blando
"""
def Remove_spacewith(s): #Elimina espacios del final
    try:        
        s.strip().rstrip().lstrip()        
    except Exception as ex:
        s='Error'
        print('Error Cleanning: {}' .format(ex))
    return s

try:    
    #SQLPLUS / AS SYSDBA 
    #SELECT NAME, con_id FROM v$pdbs;
    connection = orcCon.connect(
        user='AdminVentas',
        password='Admin123',
        dsn='localhost:1521/ORCL',  # Data Source Name
        encoding='UTF-8'
    )    
    print('Conexion establecida con ORACLE')
    print("cx_Oracle version:", orcCon.version)
    print("Database version:", connection.version)
    print("Client version:", orcCon.clientversion())
    cursor = connection.cursor()
except Exception as ex:
    print('Error Connection: {}' .format(ex))

##INICIA CURSOR    

df=pd.read_csv('C:\\...\FACTURAS_CSV_LIMPIO2.csv')

try:    
     for index, row in df.iterrows(): #Recorre las lineas del csv
         try:        
             # print('inicia proceso')
              id_innvoice=row.VNT_ID_FACTURA #Remove_spacewith(row.VNT_ID_FACTURA).strip()
              fk_costumer=row.CTL_CVE_FKCLIENTE #Remove_spacewith(row.CTL_CVE_FKCLIENTE).strip()
              fk_city=row.VNT_CVE_FKCIUDAD #Remove_spacewith(row.VNT_CVE_FKCIUDAD).strip()                
              fk_estado=row.CBR_ESTADO_FACTURA #Remove_spacewith(row.CBR_ESTADO_FACTURA).strip()      
              Monto=row.VNT_MONTO_FACTURA #Remove_spacewith(row.VNT_MONTO_FACTURA).strip()      
              Iva=row.VNT_IVA_FACTURA #Remove_spacewith(row.VNT_IVA_FACTURA).strip()      
              Date=row.VNT_FECHA_FACTURA#datetime.strptime(row.VNT_FECHA_FACTURA.replace(" 00:00\xa0",""), "%Y-%m-%d") #row.VNT_FECHA_FACTURA.replace(" 00:00","") #Remove_spacewith(row.VNT_FECHA_FACTURA)                                 
              #print("id_innvoice:"+ id_innvoice + "id_costumer:" + fk_costumer + "id_city:" + fk_city + ':' + fk_estado + ':' + Monto + ':' + Iva + 'Fesha:' + Remove_spacewith(Date) + ':')
              #print(tuple(row))
              cursor.execute("INSERT INTO VNT_FACTURACIONES(VNT_ID_FACTURA, CTL_CVE_FKCLIENTE, VNT_CVE_FKCIUDAD, CBR_ESTADO_FACTURA, VNT_MONTO_FACTURA, VNT_IVA_FACTURA, VNT_FECHA_FACTURA)VALUES(:1,:2,:3,:4,:5,:6,:7)", tuple(row))
              connection.commit()
              print('Registro insertado')
         except Exception as ex:
             print('Error loading data: {}' .format(ex))  
             #cursor.close()        
except Exception as ex:    
    print('Error Method: {}' .format(ex)) 
    #cursor.close()
except DatabaseError as e:
    err, = e.args
    print("Oracle-Error-Code:", err.code)
    print("Oracle-Error-Message:", err.message)
finally:
    cursor.close()
    connection.close()    
