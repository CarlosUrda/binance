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

    Mejorar las excepciones cambiando los assert por excepciones creadas.

    CRear un archivo de log donde ir registrando todo.
    
    Permitir que haya un número ilimitado de transacciones de grupo y e ir
    haciendo el merge sin restricción. El problema de mezclar transacciones de
    distintos grupos y que no dé error (P. ej: justo hay 3 transacciones 
    seguidas de un trading —compra, venta y comisión— pero que no pertenecen
    realmente a la misma operación. La forma de solucionarlo sería alertando
    de la diferencia entre lo que vale lo comprado y lo que vale lo vendido. Si
    la diferencia pasa cierto umbral, anotarlo en el log.

"""

import utilidades.util as u
import datetime as dt
import collections as cl
import sys
import csv



# *** FUNCIONES UTIL ***

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



def getItem(indexable, index, default=None):
    """
    Obtener el valor en una posición del indexable (secuencia o mapping).

    ARGUMENTOS:
        - indexable: secuencia o mapping del cual obtener el valor de su
        elemento index.
        - index: clave/índice del elemento del indexable a obtener su valor.
        Si indexable es secuencia, index debe ser un entero.
        - default: valor por defecto en caso de no existir el elemento en index.

    RETORNO:
        Valor del elemento de indexable en la posición index, o default si el
        indexable no tiene ningún elemento en posición index.

    EXCEPCIONES:
        TypeError si indexable es secuencia e index no es entero.
    """
    
    try:
        return indexable.get(index, default)
    except AttributeError:
        pass

    try:
        return indexable[index]
    except IndexError:
        return default



# *** FUNCIONES CSV ***

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



# *** FUNCIONES PARA OBTENER UN VALOR A PARTIR DE VARIOS ***

def applyDateFormat(strDate, dateFormat, newDateFormat):
    """
    Transformar una cadena fecha de un formato determinado a una cadena fecha
    con un nuevo formato.
 
    ARGUMENTOS:
        - strDate: Cadena con la fecha de la cual obtener el día.
        - dateFormat: Cadena representando el formato completo de la fecha.
        - newDateFormat: Cadena representando el formato de la nueva fecha.

    RETORNO:
        Cadena representando la fecha con el nuevo formato.
    """
    fechaTrxn = dt.datetime.strptime(strDate, dateFormat)
    return fechaTrxn.strftime(newDateFormat)



def joinStrValues(*values):
    """
    Concatenar varios valores, convirtiéndolos previamente a cadena, formando
    una sola cadena str como resultado.

    ARGUMENTOS:
        - values: lista de valores a concatenar.

    RETORNO:
        Concatenación de cada uno de los valores convertidos previamente a
        cadena de caracteres str.
    """
    return "".join(map(str, values))



def getValueWithDate(getValue, dateFormat, newDateFormat, date, *values):
    """
    Función que obtiene un valor a partir de un conjunto de valores usando
    un metodo pasado como argumento. Antes de obtener el valor resultado se
    cambia el formato de la fecha, la cual, una vez transformada, se incluye
    al final de la lista de valores para obtener el valor resultante.

    ARGUMENTOS:
        - getValue: función a usar para obtener un nuevo valor a partir
        de la lista de valores (incluyendo la fecha modificada al final)
        - date: Cadena str representando una fecha.
        - dateFormat: formato de la fecha.
        - newDateFormat: nuevo formato a aplicar a la fecha.
        - values: lista de valores a partir de los cuales obtener nuevo valor
        resultado.

    RETORNO:
        Valor resultado de aplicar getValue a la lista de valores,
        incluyendo la fecha modificada al final de la lista de valores.
    """
    newDate = applyDateFormat(date, dateFormat, newDateFormat)
    return getValue(*values, newDate) 



# En la función merge avisar que no ordena el resultado; se tiene que ordenar
# desde fuera de la función. Merge solo avanza a través del iterable tal y como
# lo pasan a la función.



# *** FUNCIONES PARA ENVOLVER Y HACER GENÉRICAS LAS OPERACIONES EN TRXNS ***

trxnErrors = \
        {"DOUBLE_GROUP_FEE": \
            "Dos transacciones de comisión en el mismo grupo", \
         "DOUBLE_GROUP_OP": \
            "Dos transacciones venta o compra en el mismo grupo",\
         "EMPTY_GROUP_OP": \
            "Falta transacción de venta y compra en el mismo grupo",\
         "MAX_GROUP_TRXNS": \
            "Más de 3 transacciones en el mismo grupo",
         "":""}



def getTrxnValue(trxn, getValue, *keys):
    """
    Obtener de manera genérica un valor a partir de una transacción aplicando 
    una función concreta.

    ARGUMENTOS:
        - trxn: Transacción a partir de la cual obtener el valor. Puede ser una
        secuencia o un mapping.
        - getValue: Función para procesar los valores de los campos y devolver
        un valor. Solo puede recibir valores de los campos de la transacción a
        ser procesados.
        - keys: índices o claves de la transacción cuyos valores serán usados en
        la obtención del id del bloque.

    RETORNO:
        Valor obtenido a partir de la transacción.
    """

    values = [trxn[k] for k in keys]
    return getValue(*values)



# Crear función que aplique una función al conjunto de valores de una
# transacción tras haber sido procesado cada uno de los valores. Mezcla de
# getTrxnValue y processTrxn

def getTrxnValueByType(trxn, typeKey, mapGetValueKeysByType):
    """
    Obtener de manera genérica un solo valor a partir de una transacción, pero
    el método aplicado para obtener dicho valor depende del contenido del
    campo de la transacción considerado como tipo.

    ARGUMENTOS:
        - trxn: Transacción a partir de la cual obtener el valor. Puede ser una
        secuencia o un mapping.
        - typeKey: clave o índice donde se encuentra el valor a ser considerado
        como tipo de de la transacción.
        - mapGetValueKeysByType: Diccionario donde por cada tipo (clave) existe
        una lista con un par de valores: función para procesar valores y lista
        de claves cuyos valores en la transacción serán procesados por la 
        función anterior. La función solo puede recibir como argumentos los
        valores de los campos de la transación a ser procesados.

    RETORNO:
        Valor obtenido a partir de la transacción en función de su tipo.
    """

    getValue, keys = mapGetValueKeysByType[trxn[typeKey]]
    return getTrxnValue(trxn, getValue, *keys)



def wrapGetTrxnValue(getValue, *keys):
    """
    Función que envuelve getTrxnValue para poder obtener una función que 
    obtenga el valor de una transacción recibiendo solo la transacción. Las
    claves y el método usados para obtener el valor se reciben en el wrap.

    ARGUMENTOS:
        - getValue: Función para procesar los valores de los campos y devolver
        un valor. Solo puede recibir valores de los campos de la transacción a
        ser procesados.
        - keys: índices o claves de la transacción cuyos valores serán usados
        en la obtención del valor.

    RETORNO:
        Función equivalente a getTrxnValue(trxn), la cual recibe una
        transacción y devuelve un valor a partir del método y los valores en 
        los campos de las claves recibidos en wrapGetTrxnValue.

    MEJORAS:
        Esta función podría eliminarse y donde se usa escribir directamente la
        función lambda.
    """

    return lambda trxn: getTrxnValue(trxn, getValue, *keys)



def wrapGetTrxnValueByType(typeKey, mapGetValueKeysByType):
    """
    Función que envuelve getTrxnValueByType para obtener una función que 
    reciba solo una transacción y obtenga un valor dependiendo del tipo de
    transacción. Las claves y el método usados por tipo de transacción para
    obtener el valor se reciben en el wrap.

    ARGUMENTOS:
        - typeKey: clave o índice de la transacción donde se encuentra el valor
        a ser considerado como tipo de de la transacción.
        - mapGetValueKeysByType: Diccionario donde por cada tipo (clave) existe
        una lista con un par de valores: función para procesar valores y lista
        de claves cuyos valores en la transacción serán procesados por la 
        función anterior. La función solo puede recibir como argumentos los
        valores de los campos de la transación a ser procesados.

    RETORNO:
        Función equivalente a getTrxnValueByType(trxn), la cual recibe una
        transacción y devuelve un valor dependiendo de su tipo a partir usando
        método y valores de las claves recibidos en el wrap.

    MEJORAS:
        Esta función podría eliminarse y donde se usa escribir directamente la
        función lambda.
    """

    return lambda trxn: getTrxnValueByType(trxn, typeKey, \
            mapGetValuesKeysByType)



# Los campos que no tienen valores se dejan sin clave si es diccionario. Si
# es lista se deja vacío.
def processTrxn(trxn, newKeysProcess):
    """
    A partir de una transacción obtener una nueva cambiando sus claves/índices y
    valores.

    ARGUMENTOS:
        - trxn: transacción a partir de la cual se obtendrá una nueva. Puede ser
        diccionario o secuencia.
        - newKeysProcess: diccionario/lista donde la clave/índice es cada nueva
        clave/índice de la nueva transacción y el valor es una lista de dos 
        elementos: 
            * primer elemento es la función a aplicar para obtener el nuevo
            valor para la nueva clave/índice.
            * segundo elemento es una lista de claves/índices de la transacción
            cuyos valores serán usados por la función para obtener el nuevo
            valor. La función solo puede recibir por argumentos valores a partir
            partir de los cuales obtendrá un nuevo valor para la nueva
            clave/índice. Si la función es None la lista de claves/índices
            asociada solo puede tener una clave/índice, cuyo valor en la
            transacción se tomará directamente como el nuevo valor para la
            nueva clave/índice en la nueva transacción.

    RETORNO:
        Si newKeysProcess es un diccionario, retorna una transacción de tipo
        OrderedDict. Si desea que las claves estén ordenadas, el diccionario
        pasado como argumento debe ser OrderedDict.
        Si newKeysProcess es una lista, devuelve una transacción de tipo lista.
        Los índices de la lista actúan como las nuevas claves.

    EXCEPCIONES:
        Si una función de newKeysProcess es None pero la lista de claves/índices
        asociadas no tiene un solo elemento.
    """
    try:
        newKeysProcess = newKeysProcess.items()
        outTrxn = cl.OrderedDict()
    except AttributeError:
        newKeysProcess = newKeysProcess.enumerate()
        outTrxn = [None] * len(newKeysProcess)
        
    for newKey, getValueKeys in newKeysProcess:
        getValue, keys = getValueKeys
        if getValue is None and length(keys) != 1:
            pass 
            # Lanzar excepción
        elif getValue is None
            outTrxn[newKey] = trxn[keys[0]]
            continue
        outTrxn[newKey] = getTrxnValue(trxn, getValue, keys)

    return outTrxn



def wrapProcessTrxn(newKeysProcess):
    """
    Función que envuelve processTrxn para obtener una función que 
    reciba solo una transacción y obtenga una nueva transacción usando la
    configuración de las nuevas claves recibidas en el wrap.

    ARGUMENTOS:
        - newKeysProcess: diccionario donde la clave es cada nueva clave de la
        nueva transacción y el valor es una lista de dos elementos: primer
        elemento es la función a aplicar para obtener el nuevo valor de la
        nueva clave; segundo elemento es una lista de claves de la transacción
        cuyos valores serán usados por la función para obtener el nuevo valor.
        La función solo puede recibir por argumentos los valores a partir de
        los cuales obtendrá un nuevo valor para la nueva clave. Si la función
        es None la lista de claves asociada solo puede tener una clave, cuyo 
        valor en la transacción se tomará directamente como el nuevo valor para 
        la nueva clave en la nueva transacción.

    RETORNO:
        Diccionario representando a la nueva transacción.

    EXCEPCIONES:
        Si una función de newKeysProcess es None pero la lista de claves
        asociadas no tiene un solo elemento.
    """
    return lambda trxn: processTrxn(trxn, newKeysProcess)



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
    dayFormat = "%Y-%m-$d"
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
            {"function": lambda v: applyDateFormat(v, dateFormat, dayFormat), \
             "keys": [dateFieldNameOut]}
    configTrxnGroupId = \
            {"staking": [lambda v: getValueWithDate(dateFormat, dayFormat, v),\
                [dateFieldNameOut, typeFieldNameOut, coinFieldNameOut]],

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
