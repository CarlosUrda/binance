#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generar archivo de datos de transacciones en formato correcto para poder ser
procesado por Binance, intentando agrupar las transacciones por día siempre que
sea posible (p.ej: varios staking de misma cripto en mismo día)

MEJORAS:
    Dentro de Modo gráfico:
    - Los datos de la fuente no necesitan ser guardados en memoria, solo
    mostrados. En memoria se guardan los datos que se van a grabar en el archivo
    de salida.
    - El nombre de cada campo cambiado en el archivo de salida solo se necesita
    aplicar al momento de escribir el archivo (en cada fila dict escrita). Antes
    no se necesita poner ese nombre en la lista de diccionarios. Solo saber qué
    nombre de campo se refiere a qué campo.

"""

import utilidades.util as u
import datetime as dt
import sys
import csv




def changeKeys( dictIn, mapKeys = dict())
"""
Cambiar las claves de un diccionario.

ARGUMENTOS:
    - dictIn: diccionario a cambiar sus claves
    - mapKeys: diccionario donde en cada par se indica una nueva clave (valor)
    para una clave antigua (clave). Si una clave antigua no tiene clave para
    mapear en mapKeys, en el nuevo diccionario se mantiene la antigua

RETORNO:
    Nuevo diccionario con las claves cambiadas. Si mapKeys == dict() devuelve
    una copia del diccionario dictIn.
"""
    return {mapKeys.get(k, k):v for k,v in dictIn.items()}



def csvOpen(file, mode="r", extrasaction="ignore", dialect=None, isDict=True, \
        fieldnames=None):
    """
    Abrir un archivo csv para acceder a sus datos parseados.
    """
    if dialect is None and 'r' == mode:
        dialect = csv.Sniffer().sniff(file.read(1024))
        file.seek(0)
    elif dialect is None and "w" == mode: 
        dialect = "excel"
    elif "r" != mode != "w" :
        pass # Lanzar excepción de argumento mode incorrecto

    # Revisar los modos en open por si hay más que r o w, o con más 
    # modificaciones como w+
    if isDict:
        csvParser = csv.DictReader if 'r' in mode else csv.DictWriter
        args = {"dialect":dialect, "fieldnames":fieldnames, \
                "extrasaction":extrasaction}
    else:
        csvParser = csv.reader if 'r' in mode else csv.writer
        args = {"dialect":dialect}

    # qué sucede si se le pasa dialect=None para saber qué hacer si modo = w:
    # comprobar antes error o dejarlo a csvParser.
    # o saber qué error da si se intenta file.read sobre un archivo modo w
    return csvParser(file, **args)



def csvWriteRows(csvWriter, rows, mapFieldNames=None):
    """
    Escribir en objeto writer csv una lista de filas.

    ARGUMENTOS:
    - csvWriter: objeto csv writer destino donde escribir.
    - rows: lista de filas a escribir. Si csvWriter es writer, rows deben de ser
    listas; si csvWriter es DictWriter, rows den de ser diccionarios. 
    - mapFieldNames: Diccionario donde se asocia un nuevo nombre para cada
    campo del diccionario de cada fila. La clave representa el nombre
    antiguo de campo, y el valor el nuevo nombre. Si None, los nombres de
    campo no cambian y se mantienen igual a los nombres de los campos de entrada

    MEJORAS:
        - Gestionar errores.
    """

    # Meter todo el código en un try para lanzar excepciones.
    if mapFieldNames is None:
        return csWriter.writerows(rows)

    for row in rows:
        csvWriter.writerow(changeKeys(row, mapFieldNames))
    return



def csvProcessTrxns(trxnsIn, dateIndex, dateFormat, typeIndex, coinIndex, \
        mapTrxnTypes, csvOut=None, fieldNamesInOut=dict()):
    """
    Procesar todas las transacciones.

    ARGUMENTOS:
        - trxnsIn: Iterator con las transacciones de entrada.
        - dateIndex: Índice del campo (transacción es lista) o nombre del campo
        (transacción es diccionario) donde se encuentra la fecha en cada trans.
        - dateFormat: Formato de la fecha.
        - typeIndex: Índice del campo (transacción es lista) o nombre del campo
        (transacción es diccionario) donde se encuentra el tipo de operación de
        la transacción realizada.
        - coinIndex: Índice del campo (transacción es lista) o nombre del campo
        (transacción es diccionario) donde se encuentra la moneda de la
        transacción realizada.
        - mapTrxnTypes = diccionario donde se relaciona cada tipo de operación
        estándar con su nombre dado en el csv de entrada. Los tipos de operación
        usados como claves son: "staking", "operation", 
        - csvOut: writer csv de salida donde escribir las transacciones
        procesadas. Si None, todas las transacciones procesadas se devuelven 
        como una lista sin escribirlas en ningún csv.
        
        Si existe un writer hay un ahorro de memoria durante el procesamiento al
        no guardarse todas las transacciones procesadas: solo van almacenando 
        temporalmente las transacciones procesadas de un día y, al pasar a una
        transacción de entrada con un día distinto, todas las transacciones 
        procesadas del día almacenadas hasta ese momento se escriben en el
        writer y se limpian de la memoria. Por esta razón, para que el
        procesamiento sea correcto usando esta manera, la lista de transacciones
        de entrada deben de estar agrupadas por día.
        Para realizar el procesamiento sin tener en cuenta la agrupación de las
        transacciones por día, se necesita almacenar en memoria todas las
        transacciones procesadas (no solo las de un día temporalmente), ya que
        hasta que no se recorran todas las transacciones de entrada no se puede
        confirmar si las transacciones procesadas de cualquier día están
        completas. Por lo tanto, para escribir las transacciones procesadas a un
        csv a partir de unas transacciones de entrada no agrupadas por día,
        antes se debe obtener la lista de todas las transacciones procesadas, y
        justo es esa lista la que devuelve la función, pudiéndose realizar la
        escritura desde fuera directamente.
        - fieldNamesInOut: Diccionario donde se asocia un nuevo nombre para cada
        campo del diccionario de cada transacción. La clave representa el nombre
        antiguo de campo, y el valor el nuevo nombre. Si None, los nombres de
        campo no cambian y se mantienen igual a los nombres de los campos de
        entrada.

    RETORNO:
        - csvOut == None: lista resultante de todas las transacciones de
        entrada procesadas. No es necesario que las transacciones de entrada
        estén ordenadas.
        - csvOut != None: True tras el procesamiento de las transacciones y
        su escritura completa en el csv de salida. Es necesario que las
        transacciones de entrada estén agrupadas por día para que todo el
        procesamiento sea coherente. 

    MEJORAS:
        - Desarrollar la opción de obtener la lista de transacciones procesadas
        directamente en la lista de entrada modificándola, en lugar de una nueva
        lista partiendo de una copia de la lista de entrada.
        - Solucionar las cadenas mágicas de los tipos de operación.
        - Borrar de las transacciones procesadas los campos que no aparezcan
        en los campos finales resultado.
        
    """
    if (csvOut is not None): 
        trxnPrevDay = False
    trxnsOut = []
    findDayTrxns = dict()

    for trxn in trxnsIn:
        fechaTrxn = dt.datetime.strptime(trxn[dateIndex], dateFormat)
        trxnDay = fechaTrxn.strftime("%d-%m-%Y")
        if (csvOut is not None and trxnPrevDay and trxnPrevDay != trxnDay):
            csvWriteRows(csvOut, trxnsOut)#, fieldNamesInOut):
            trxnPrevDay = trxnDay
            trxnsOut = []
            findDayTrxns = dict()

        trxnType = trxn[typeIndex]
        if (trxnType == mapTrxnTypes["staking"])
            processedTrxn = dget(findDayTrxns, [trxnDay, trxnType, trxn[coinIndex]], False)
            if processedTrxn:
                processedTrxn[fieldNamesInOut[valueIndex]] += trxn[valueIndex] 
                continue
            processedTrxn = changeKeys(trxn, fieldNamesInOut)
            dset(findDayTrxns, [trxnDay, trxnType, trxn[coinIndex]], trxnNew)

        findDayTrxns[trxnDay]

       # Saber si la transacción se puede agrupar con otras del mismo día.
       # Si no se agrupa, se crea el nuevo dict o se obtiene el existente 
       # modificado
       # Se registra el dict tanto en la lista de transacciones del día como
       # en el dict para saber si ya existe



def main():
    """
    Función principal.

    El programa permite cuatro combinaciones entrada/salida: Procesar las
    transacciones directamente desde un reader csv o desde una lista de
    transacciones de entrada almacenadas ya en memoria hacia un writer csv o 
    para almacenarlas en otra lista de transacciones de salida en memoria.
    
    MEJORAS:
        - Dar la opción de ir escribiendo en el archivo de salida cada día de
        transacciones procesado o, en cambio, escribir todas las transacciones
        de golpe.
        Las opciones que maneja el programa son:
        - Aagrupar transacciones por día para reducir el número.
        - Elegir el orden de los campos de salida.
        - Cambiar el nombre de los campos de salida.
        - Eliminar campos de salida.
    """
    # comprobar errores de entrada
    fileNameIn = sys.argv[1]
    fileNameOut = sys.argv[2]

    # Los siguientes valores se podrán meter como parámetros al programa, sobre
    # todo al usar interfaz gráfica. Por defecto valores siguientes:
    dateFormat = "%Y-%m-$d %H:%M:%S"
    dateFieldNameIn = "UTC_Time"
    coinFieldnameIn = "Coin"
    typeFieldNameIn = "Operation"
    valueFieldNameIn = "Change"
    dateFieldNameOut = "Fecha"
    coinFieldnameOut = "Moneda"
    typeFieldNameOut = "Operacion"
    valueFieldNameOut = "Cantidad"
    commentFieldNameOut = "Comentario"
    
    fieldNamesOut = [] # Nombres de los campos en el archivo csv de salida.
    fieldNamesInOut = {dateFieldName: "Fecha", typeFieldName: "Operacion", \
            coinFieldName: "Moneda", valueFieldName: "Cantidad"}
    isCsvInToMem = True
    isCsvOutToMem = True
    
    with open(fileNameIn, newline='') as fileIn:
        csvIn = csvOpen(fileIn, 'r', isDict=True)
        trxnsIn = [trxn for trxn in csvIn] if isDumpCSVtoMem else csvIn

        
        trxnsOut = csvProcessTrxns(trxnsIn, dateFieldName, dateFormat, \
                isCsvOutToMem, csvOut, fieldNamesInOut)

    
    csvOut = csvOpen(fileNameOut, 'w', dialect="excel", isDict=True, \
            fieldnames=fieldNamesOut)
    csvOut.writeheader()
    csvWriteRows(csvOut, trxnsOut, fieldNamesInOut)

    # cerrar el archivo csv abierto



if __name__ in ("__main__", "__console__"):
    main()
