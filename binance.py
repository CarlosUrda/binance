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
import collections as cl
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



def applyFormatToDate(strDate, dateFormat, newDateFormat):
    """
    Transformar una cadena fecha de un formato determinado a una cadena fecha
    con un nuevo formato.
 
    ARGUMENTOS:
        - strDate: Cadena con la fecha de la cual obtener el día.
        (transacción es diccionario) donde se encuentra la fecha en cada trans.
        - dateFormat: Cadena representando el formato completo de la fecha.
        - newDateFormat: Cadena representando el formato de la nueva fecha.

    RETORNO:
        Cadena representando la fecha con el nuevo formato.
    """
    fechaTrxn = dt.datetime.strptime(strDate, dateFormat)
    return fechaTrxn.strftime(newDateFormat)



def getGroupId(*values):
    """
    Función que obtiene un sencillo groupId a partir de un conjunto de valores.

    ARGUMENTOS:
        - values: lista de valores a partir de los cuales obtener el groupId.

    RETORNO:
        GroupId formado simplemente por la concatenación de cada uno de los
        valores convertidos previamente a cadena de caracteres.
    """
    return "".join([str(value) for v in values])



def getNewDateGroupId(dateIndex, dateFormat, newDateFormat, *values):
    """
    Función que obtiene un sencillo groupId a partir de un conjunto de valores.
    Antes de realizar el groupId transforma el valor de la fecha de uno de los
    valores a un nuevo formato.

    ARGUMENTOS:
        - dateIndex: índice de la lista de valores que contiene la fecha.
        - dateFormat: formato de la fecha de la lista de valores.
        - newDateFormat: nuevo formato a aplicar a la fecha.
        - values: lista de valores a partir de los cuales obtener el groupId.

    RETORNO:
        GroupId formado simplemente por la concatenación de cada uno de los
        valores convertidos previamente a cadena de caracteres.
    """
    values[dateIndex] = applyFormatToDate(values[dateIndex], dateFormat, \
            newDateFormat)
    return getGroupId(*values) 




# En la función merge avisar que no ordena el resultado; se tiene que ordenar
# desde fuera de la función. Merge solo avanza a través del iterable tal y como
# lo pasan a la función.

def getTrxnValue(trxn, processValues, *keys):
    """
    Obtener de manera genérica un solo valor a partir de una transacción.

    ARGUMENTOS:
        - trxn: Transacción a partir de la cual obtener el valor. Puede ser una
        secuencia o un mapping.
        - processValues: Función para procesar los valores de los campos y
        devolver un valor.
        - keys: índices o claves de la transacción cuyos valores serán usados en
        la obtención del id del bloque.

    RETORNO:
        Valor obtenido a partir de la transacción.
    """
    values = [trxn[k] for k in keys]
    return processValue(*values)



def getTrxnValueByType(trxn, typeKey, mapProcessKeysByType):
    """
    Obtener de manera genérica un solo valor a partir de una transacción, pero
    el método aplicado para obtener dicho valor depende del contenido del
    campo de la transacción considerado como tipo.

    ARGUMENTOS:
        - trxn: Transacción a partir de la cual obtener el valor. Puede ser una
        secuencia o un mapping.
        - typeKey: clave o índice donde se encuentra el valor a ser considerado
        como tipo de de la transacción.
        - mapProcessKeysByType: Diccionario donde por cada tipo (clave) existe
        una lista con un par de valores: método para procesar valores y lista
        de claves cuyos valores en la transacción serán procesados por la 
        función anterior.

    RETORNO:
        Valor obtenido a partir de la transacción en función de su tipo.
    """
    processValues, keys = mapProcessKeysByType[trxn[typeKey]]
    return getTrxnValue(trxn, processValues, *keys)



def wrapGetTrxnBlockId(processValues, *keys):
    """
    Función que envuelve getTrxnValue para hacer de función que obtiene el 
    identificador del bloque de las transacciones al que pertenece una 
    transacción. De esta manera, hace de alias de getTrxnBlockId sin necesidad 
    de tener fijos dentro de la función los campos y método usados para obtener
    el id de bloque cada transacción.

    ARGUMENTOS:
        - processValue: Función para procesar los valores de los campos y
        devolver un id del bloque.
        - keys: índices o claves de la transacción cuyos valores serán usados en
        la obtención del id del bloque.

    RETORNO:
        Función equivalente a getTrxnBlockId(trxn), la cual recibe una
        transacción y devuelve el id de su bloque a partir del método y los
        valores de las claves recibidos en wrapGetTrxnBlockId.
    """
    return lambda trxn: getTrxnValue(trxn, processValues, *keys)



def wrapGetTrxnGroupId(typeKey, mapProcessKeysByType):
    """
    Función que envuelve getTrxnValueByType para hacer de función que obtiene el
    groupId de la transacción en función del tipo de transacción. De esta manera
    hace de alias de getTrxnGroupId sin necesidad de tener fijos dentro de la 
    función los campos y método usados para obtener el groupId del transacción.

    ARGUMENTOS:
        - typeKey: clave o índice donde se encuentra el valor a ser considerado
        como tipo de de la transacción.
        - mapProcessKeysByType: Diccionario donde por cada tipo (clave) existe
        una lista con un par de valores: método para procesar valores y lista
        de claves cuyos valores en la transacción serán procesados por la 
        función anterior.

    RETORNO:
        Función equivalente a getTrxnGroupId(trxn), la cual recibe una
        transacción y devuelve el groupId por su tipo a partir del método y los
        valores de las claves recibidos en wrapGetTrxnGroupId.
    """
    return lambda trxn: getTrxnValueByType(trxn, typeKey, mapProcessKeysByType)




def processTrxn(trxn):


def mergeTrxnsByGroup():


# El valor del tipo usado para obtener la clave groupId debe estar al inicio o
# final de los valores usados para obtener groupId. Esto se hace para evitar que
# dos groupId de dos transacciones con tipos distinto (obtenidos a partir de 
# distintos valores) no coinciddan de ninguna manera.

# Los valores del tipo de transacción deben ser un inmutable, ya que si no, no
# puede usarse como clave en el diccionario pasado a getTrxnValueByType.

def csvProcessTrxns(trxnsIn, processTrxn, csvOut=None, mergeTrxnsByGroup=None,
        getGroupId=None, getBlockId=None)
    """
    Procesar todas las transacciones.

    ARGUMENTOS:
        - trxnsIn: Iterator con las transacciones de entrada.
        - csvOut: writer csv de salida donde escribir las transacciones
        procesadas. Si None, todas las transacciones procesadas se devuelven 
        como una lista sin escribirlas en ningún csv.
        Si existe un writer no se guardan todas las transacciones procesadas:
        solo van almacenando temporalmente las transacciones procesadas de un 
        día y, al pasar a una transacción de entrada con un día distinto, todas 
        las transacciones procesadas del día almacenadas hasta ese momento se 
        escriben en el writer csv y se limpian de la memoria. Por esta razón,
        para que el procesamiento sea correcto usando esta manera, la lista de 
        transacciones de entrada deben de estar agrupadas por día; además, no
        puede haber transacciones de días distintos que sean potencialmente
        unibles en una sola transacción.
        Esta opción existe para ahorrar memoria, pero tienen que cumplirse dos
        condiciones para que el procesamiento sea correcto: que la lista de 
        transacciones esté ordenada por día, y que no haya varias transacciones
        unibles en una sola transacción que pertenezcan a días distintos. Si
        alguna de las dos condiciones no se cumple, es mejor no pasar ningún
        csv writer a la función, que devuelva la lista completa de transacciones
        procesadas y, desde fuera de la función, escribir el resultado a un 
        csv writer.
        - fieldNamesInOut: Diccionario donde se asocia un nuevo nombre para cada
        campo del diccionario de cada transacción. La clave representa el nombre
        antiguo de campo, y el valor el nuevo nombre. Si None, los nombres de
        campo no cambian y se mantienen igual a los nombres de los campos de
        entrada.
        Valor por el que están aglutinadas u ordenadas las transacciones de
        entrada formando bloques en memoria. Sirve para optimizar la memoria
        al ir almacenando solamente cada vez un solo bloque de transacciones que
        tienen este mismo valor. Al usarse esta opción, la unión de 
        transacciones (merge) deben ser solo entre las transacciones de un mismo
        bloque

    RETORNO:
        - csvOut == None: lista resultante de todas las transacciones de
        entrada procesadas.
        estén agrupadas por día.
        - csvOut != None: Número de caracteres escritos en el csw writer.

    MEJORAS:
        
    """
    groupIdTrxns = cl.OrderedDict()
    outTrxns = [] if (csvOut is None) else 0
    prevBlockId = None
    doMerge = mergeTrxnsByGroup is not None and getGroupId is not None
    doBlocks = getBlockId is not None

    for trxn in trxnsIn:
        trxn = processTrxn(trxn)

        if (not doMerge):
            if (csvOut is None)
                outTrxns.append(trxn)
            else:
                outTrxns += csvOut.writerow(trxn)
            continue

        if (doBlocks):
            blockId = getBlockId(trxn)
        if (prevBlockId is not None and prevBlockId != blockId):
            tempOutTrxns = mergeTrxnsByGroup(groupIdTrxns.values())
            if (csvOut is None):
                outTrxns.extend(tempOutTrxns)
            else:
                outTrxns += csvOut.writerows(tempOutTrxns)
            prevBlockId = blockId
            groupIdTrxns = cl.OrderedDict()

        groupId = getGroupId(trxn)
        if (groupId not in groupIdTrxns)
            groupIdTrxns[groupId] = []
        groupIdTrxns[groupId].append(trxn)

    if (doMerge):
        tempOutTrxns = mergeTrxnsByGroup(groupIdTrxns.values())
        if (csvOut is None):
            outTrxns.extend(tempOutTrxns)
        else:
            outTrxns += csvOut.writerows(tempOutTrxns)
    
    return outTrxns 




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
    newDateFormat = "%Y-%m-$d"
    dateFieldNameIn = "UTC_Time"
    coinFieldnameIn = "Coin"
    typeFieldNameIn = "Operation"
    valueFieldNameIn = "Change"
    dateFieldNameOut = "Fecha"
    coinFieldnameOut = "Moneda"
    typeFieldNameOut = "Operacion"
    valueFieldNameOut = "Cantidad"
    commentFieldNameOut = "Comentario"
    
    configTrxnBlockId = \
            {"function": lambda d: applyFormatToDate(d, dateFormat, \
                newDateFormat), \
             "keys": [dateFieldNameOut]}
    configTrxnGroupId = \
            {"staking": [lambda v: getNewDateGroupId(2, dateFormat, newDateFormat, v), [typeFieldNameOut, coinFieldNameOut, dateFieldNameOut]],

    fieldNamesOut = [] # Nombres de los campos en el archivo csv de salida.
    fieldNamesInOut = {dateFieldName: "Fecha", typeFieldName: "Operacion", \
            coinFieldName: "Moneda", valueFieldName: "Cantidad"}
    isCsvInToMem = True
    isCsvOutToMem = True
    
    with open(fileNameIn, newline='') as fileIn:
        csvIn = csvOpen(fileIn, 'r', isDict=True)
        trxnsIn = [trxn for trxn in csvIn] if isDumpCSVtoMem else csvIn

        # Dar antes la opción de agrupar las transacciones itertools groupby 
        outTrxns = csvProcessTrxns(trxnsIn, dateFieldName, dateFormat, \
                isCsvOutToMem, csvOut, fieldNamesInOut)

    
    csvOut = csvOpen(fileNameOut, 'w', dialect="excel", isDict=True, \
            fieldnames=fieldNamesOut)
    csvOut.writeheader()
    csvWriteRows(csvOut, outTrxns, fieldNamesInOut)

    # cerrar el archivo csv abierto



if __name__ in ("__main__", "__console__"):
    main()
