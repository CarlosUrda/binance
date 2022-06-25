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

    Dar la opción de que el programa agrupe todas las transacciones de entrada
    correctamente antes de empezar a procesarlas.

"""

import utilidades.util as u
import datetime as dt
import collections as cl
import logging as log
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




def wrapf(function, *endArgs, **kwEndArgs):
    """
    Envolver una función para obtener esa misma función con parte de sus
    argumentos ya fijados. 

    ARGUMENTOS:
        - function: función a envolver.
        - endArgs: argumentos a ser fijados al inicio de la función tras los
        argumentos de entrada al llamarla.
        - kwEndArgs: argumentos a ser fijados al final de la función tras los
        argumentos con nombre al llamarla.

    RETORNO:
        function con parte de sus argumentos ya fijados por endArgs y kwEndArgs.
    """

    return lambda *startArgs, **kwStartArgs: function(*(startArgs+endArgs), \
            **dict(kwStartArgs, **kwEndArgs))





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




def getParsedValue(*values, getValue=joinStrValues, valueParses=None):
    """
    Obtener un valor a partir de una serie de valores. Antes de obtener el valor
    se pueden parsear cada uno de los valores. Los nuevos valores obtenidos son
    los que usarán para generar el valor.

    ARGUMENTOS:
        - values: valores para generar el nuevo valor.
        - getValue: función para obtener el valor a partir de los valores
        ya parseados. Los argumentos pasados a esta función son typeValue
        en primero lugar, seguido de los valores en values.
        - valueParsers: diccionario con las funciones a usar para parsear los
        valores. La clave de cada parser tiene que ser un entero (o convertible
        a entero) e indica la posición del argumento a parsear
        0 se refiere a typeValue.

    RETORNO:
        Valor resultado de aplicar getValue a la lista de valores parseados.
    """
   
    if valueParsers is not None:
        for index, parser in valueParsers.items():
            values[index] = parser(value[int(index)])

    getValue(*values)




# Intentar hacer las funciones que obtienen valores de grupo de manera genérica.
def getGroupId(typeValue, *values, getValue=joinStrValues, valueParsers=None):
    """
    Obtener el valor que representa el groupId a partir de los valores de una
    serie de campos pertenecientes a una transacción. Antes de obtener el valor
    groupId se pueden parsear cada uno de los valores. Los nuevos valores
    obtenidos son los que usarán para generar el groupId.
    Tener en cuenta que getValue debe obtener un valor único para cada cjto de
    valores (typeValue+values) pasado.

    ARGUMENTOS:
        - typeValue: tipo de transacción. Es necesario siempre para evitar que
        transacciones de distintos tipos generen mismos groupId.
        - values: resto de valores para obtener el groupId.
        - getValue: función para obtener el valor a partir de los valores
        ya parseados. Los argumentos pasados a esta función son typeValue
        en primero lugar, seguido de los valores en values.
        - valueParsers: diccionario con las funciones a usar para parsear los
        valores. La clave de cada parser tiene que ser un entero (o convertible
        a entero) e indica la posición del argumento a parsear
        0 se refiere a typeValue.

    RETORNO:
        GroupId resultado de aplicar getValue a la lista de valores parseados.
    """
    values.insert(0, typeValue)
    getParsedValue(*values, getValue=getValue, valueParsers=valueParsers)




# En la función merge avisar que no ordena el resultado; se tiene que ordenar
# desde fuera de la función. Merge solo avanza a través del iterable tal y como
# lo pasan a la función.



# *** FUNCIONES PARA ENVOLVER Y HACER GENÉRICAS LAS OPERACIONES EN TRXNS ***

trxnErrors = \
        {"DOUBLE_GROUP_FEE": \
            "Dos transacciones de comisión en el mismo grupo", \
         "DOUBLE_GROUP_OP": \
            "Dos transacciones venta o compra en el mismo grupo", \
         "EMPTY_GROUP_OP": \
            "Falta transacción de venta o compra en el mismo grupo", \
         "NUM_GROUP_TRXNS": \
            "Número incorrecto de transacciones en el grupo.", \
         "TYPE_GROUP_TRXNS": \
            "Tipo incorrecto de transacciones en el grupo.", \
         "COIN_GROUP_STAKING": \
            "Distintas monedas al agrupar por staking", \
         "DUP_ASIGN_PROCESS_TRXN": \
            "No se puede asignar a un campo en la nueva transacción \
            directamente el valor de más de un campo de la antigua transacción"
        }




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

    values = [getItem(trxn, k, "") for k in keys]
    return getValue(*values)




# Crear función que aplique una función al conjunto de valores de una
# transacción tras haber sido procesado cada uno de los valores. Mezcla de
# getTrxnValue y processTrxn

def getTrxnValueByType(trxn, fieldKey, mapGetValueKeysByType):
    """
    Obtener de manera genérica un solo valor a partir de una transacción, pero
    el método aplicado para obtener dicho valor depende del contenido de un
    campo de la transacción.

    ARGUMENTOS:
        - trxn: Transacción a partir de la cual obtener el valor. Puede ser una
        secuencia o un mapping.
        - fieldKey: clave o índice donde se encuentra el valor a ser considerado
        como campo referencia de la transacción.
        - mapGetValueKeysByType: Diccionario donde por cada tipo (clave) existe
        una lista con un par de valores: función para procesar valores y lista
        de claves cuyos valores en la transacción serán procesados por la 
        función anterior. La función solo puede recibir como argumentos los
        valores de los campos de la transación a ser procesados.

    RETORNO:
        Valor obtenido a partir de la transacción en función de su tipo.
    """

    getValue, keys = mapGetValueKeysByType[trxn[fieldKey]]
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
        - newKeysProcess: diccionario/lista donde cada clave/índice es la nueva
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
        Si newKeysProcess es una secuencia, devuelve una transacción de tipo
        lista. Los índices de la lista actúan como las nuevas claves.

    EXCEPCIONES:
        Si una función de newKeysProcess es None pero la lista de claves/índices
        asociadas no tiene solo un elemento.
    """

    try:
        newKeysProcess = newKeysProcess.items()
        outTrxn = cl.OrderedDict()
    except AttributeError:
        newKeysProcess = newKeysProcess.enumerate()
        outTrxn = [None] * len(newKeysProcess)
        
    for newKey, getValueKeys in newKeysProcess:
        getValue, keys = getValueKeys
        if keys is None:
            keys = []
        assert getValue is None and length(keys) != 1, \
                trxnErrors["DUP_ASIGN__PROCESS_TRXN"] + f": {keys}"
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




def mergeStakingTrxns(trxns, coinIndex, stakedIndex):
    """
    Unir transacciones de tipo staking. 

    ARGUMENTOS:
        - trxns: lista de transacciones a unir como un solo staking. Las
        transacciones de la lista serán modificados in-place.
        - coinIndex: moneda conseguida en staking.
        - stakedIndex: clave/índice de la transacción donde se encuentra la
        el valor obtenido en staking.

    RETORNO:
        Devuelve la transacción resultado de la unión, que es la transacción
        base modificada in-place. El valor resultado de la unión se guarda como
        str.

    EXCEPCIONES:
        Si el tipo de moneda de dos transacciones es distinta se lanza excepción
    """

    assert not trxns, trxnErrors["NUM_GROUP_TRXNS"] + ": 0"

    trxnOut = trxns.pop(0)
    trxnsOut = [trxnOut]
    for trxn in trxns:
        assert trxnOut[coinIndex] != trxn[coinIndex], \
                    trxnErrors["COIN_GROUP_STAKING"] + \
                    f": {trxnOut[coinIndex]}, {trxn[coinIndex]}"

        trxnOut[stakedIndex] = float(trxnOut[stakedIndex]) + \
                float(trxn[stakedIndex]))

    trxnOut[stakedIndex] = str(trxnOut[stakedIndex])
    return trxnsOut




def mergeTradeTrxns(trxns, buyCoinIndex, buyValueIndex, sellCoinIndex, \
        sellValueIndex, feeCoinIndex, feeValueIndex, commentIndex):
    """
    Unir hasta tres transacciones de tipo trading. Si la transacción comisión
    no puede unirse se devuelve como trxns aparte.

    ARGUMENTOS:
        - trxns: lista de transacciones del mismo grupo trading. 
        - buyCoinIndex: clave/índice de la moneda de compra.
        - buyValueIndex: clave/índice de la cantidad de moneda comprada.
        - sellCoinIndex: clave/índice de la moneda de venta.
        - sellValueIndex: clave/índice de la cantidad de moneda vendida.
        - feeCoinIndex: clave/índice de la moneda de comisión.
        - feeValueIndex: clave/índice de la cantidad de moneda en comisión.
        - commentIndex: clave/índice del comentario.

    RETORNO:
        Devuelve lista con transacciones resultado de unirlas. Si la transacción
        comisión no se puede unir se devuelve dentro de la lista.
        La lista de transacciones de entrada quedan modificadas.

    EXCEPCIONES:
        Si el número de transacciones del grupo es mayor que 3
        Si hay compra, venta o comisión repetida dentro de las transacciones.
        Si falta compra o venta en las transacciones.
    """

    trxnsNum = len(trxns)
    assert not trxns or trxnsNum > 3, \
            trxnErrors["NUM_GROUP_TRXNS"] + ": " + len(trxns)

    feeTrxn = outTrxn = None
    for trxn in trxns:
        feeCoin = getItem(trxn, feeCoinIndex, "")
        assert (feeCoin != "" and feeTrxn is not None), \
                trxnErrors["DOUBLE_GROUP_FEE"] + f": {feeTrxn} | {trxn}"
        if feeCoin != "":
            assert trxnsNum == 2, \
                    trxnErrors["EMPTY_GROUP_OP"] + f": {trxn}"
            if trxnsNum == 1
                return [trxn]
            feeTrxn = trxn
            continue
        
        if outTrxn is None:
            assert trxnsNum == 1, \
                    trxnErrors["EMPTY_GROUP_OP"] + f": {trxn}"
            outTrxn = trxn
            outTrxns = [outTrxn]
            continue

        for coinIndex, valueIndex in {buyCoinIndex: buyValueIndex,
                sellCoinIndex: sellValueIndex}:
            inCoin = getItem(trxn, coinIndex, "")
            outCoin = getItem(outTrxn, coinIndex, "")
            assert (outCoin != "" and inCoin != ""), \
                    trxnErrors["DOUBLE_GROUP_OP"] + f": {outTrxn} | {trxn}"
            assert outCoin == "" and inCoin == "", \
                    trxnErrors["EMPTY_GROUP_OP"] + f": {outTrxn} | {trxn}"
            if outCoin == "" and inCoin != "":
                trxnOut[coinIndex] = inCoin
                trxnOut[valueIndex] = getItem(trxn, valueIndex, "")
  
    if feeTrxn is None:
        return outTrxns
    
    if getItem(outTrxn, buyCoinIndex) == getItem(feeTrxn, feeCoinIndex):
        buyValue = float(getItem(outTrxn, buyValueIndex, 0))
        feeValue = float(getItem(feeTrxn, feeValueIndex, 0))
        if buyValue > feeValue:
            outTrxn[buyValueIndex] = str(buyValue - feeValue)
            outTrxn[feeCoinIndex] = getItem(feeTrxn, feeCoinIndex)
            outTrxn[feeValueIndex] = getItem(feeTrxn, feeValueIndex)
            return outTrxns
    elif getItem(outTrxn, sellCoinIndex) == getItem(feeTrxn, feeCoinIndex):
        sellValue = float(getItem(outTrxn, sellValueIndex, 0))
        feeValue = float(getItem(feeTrxn, feeValueIndex, 0))
        outTrxn[sellValueIndex] = str(sellValue + feeValue)
        outTrxn[feeCoinIndex] = getItem(feeTrxn, feeCoinIndex)
        outTrxn[feeValueIndex] = getItem(feeTrxn, feeValueIndex)
        return outTrxns

    feeTrxn[commentIndex] = f"Comisión por transacción \
            {outTrxn[sellCoinIndex]}=>{outTrxn[buyCoinIndex]}"
    outTrxns.append(feeTrxn)
    return outTrxns




def mergeDustTrxns(trxns, buyCoinIndex, buyValueIndex, sellCoinIndex, \
        sellValueIndex, commentIndex)
    """
    Unir dos transacciones de tipo dust.

    ARGUMENTOS:
        - trxns: lista de transacciones del mismo grupo dust.
        - buyCoinIndex: clave/índice de la moneda de compra.
        - buyValueIndex: clave/índice de la cantidad de moneda comprada.
        - sellCoinIndex: clave/índice de la moneda de venta.
        - sellValueIndex: clave/índice de la cantidad de moneda vendida.
        - commentIndex: clave/índice del comentario.

    RETORNO:
        Devuelve lista con transacciones resultado de unirlas. Si la transacción
        comisión no se puede unir se devuelve dentro de la lista.
        La lista de transacciones de entrada quedan modificadas.

    EXCEPCIONES:
        Si no hay dos transacciones.
        Si falta compra o venta en las transacciones.
    """

    trxnsNum = len(trxns)
    assert not trxns or trxnsNum != 2, \
            trxnErrors["NUM_GROUP_TRXNS"] + ": " + len(trxns)

    outTrxn = None
    for trxn in trxns:
        if outTrxn is None:
            outTrxn = trxn
            outTrxns = [outTrxn]
            continue

        for coinIndex, valueIndex in {buyCoinIndex: buyValueIndex,
                sellCoinIndex: sellValueIndex}:
            inCoin = getItem(trxn, coinIndex, "")
            outCoin = getItem(outTrxn, coinIndex, "")
            assert (outCoin != "" and inCoin != ""), \
                    trxnErrors["DOUBLE_GROUP_OP"] + f": {outTrxn} | {trxn}"
            assert outCoin == "" and inCoin == "", \
                    trxnErrors["EMPTY_GROUP_OP"] + f": {outTrxn} | {trxn}"
            if outCoin == "" and inCoin != "":
                trxnOut[coinIndex] = inCoin
                trxnOut[valueIndex] = getItem(trxn, valueIndex, "")
  
    outTrxns.append(feeTrxn)
    return outTrxns




# Cada groupId debe ser único, sean los grupos del mismo tipo o no
def mergeTrxnsGroupsByField(trxnsGroups, fieldIndex, fieldMergesKeys):
    """
    Realiza la unión, por cada grupo, de una lista de transacciones candidatas
    a ser juntadas en una sola transacción. El método usado para realizar el
    merge de las transacciones depende de un campo de la transacción.

    ARGUMENTOS:
        - trxnsGroups: lista de listas o grupos de transacciones a realizar 
        merge por grupo.
        - fieldIndex: clave/índice de la transacción donde se encuentra el valor
        del campo.
        - fieldMergesKeys: diccionario que empareja cada tipo de transacción con
        una lista que contiene el método a usar para unir las transacciones del
        mismo grupo y una lista de claves/índices de la transacción cuyos
        valores serán necesarios para aplicar el merge correctamente.

    RETORNO:
        Transacción resultado de la unión de las transacciones del mismo grupo.
    """
   
    outTrxns = []
    for trxnsGroup in trxnsGroups:
        groupField = trxnsGroup[0][fieldIndex]
        mergesKeys = fieldMergesKeys[groupField]
        try:
            outTrxns.extend(mergesKeys[0](trxnsGroup, *mergesKeys[1]))
        except BaseException as e:
            log.exception(f"Error merge grupo {groupField}: {trxnsGroup}")
            raise e
            # Lanzar excepción creada

    return outTrxns




def wrapMergeGroupTrxnsByType(typeIndex, typeMerges):
    """
    """

    return lambda trxns: mergeGroupTrxnsByType(trxns, typeIndex, typeMerges)








# El valor del tipo usado para obtener la clave groupId debe estar al inicio o
# final de los valores usados para obtener groupId. Esto se hace para evitar que
# dos groupId de dos transacciones con tipos distinto (obtenidos a partir de 
# distintos valores) no coinciddan de ninguna manera.

# Los valores del tipo de transacción deben ser un inmutable, ya que si no, no
# puede usarse como clave en el diccionario pasado a getTrxnValueByType.

def csvProcessTrxns(trxnsIn, processTrxn, csvOut=None, \
        mergeTrxnsGroupsByType=None, getGroupId=None, getBlockId=None)
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
    trxnsGroups = cl.OrderedDict()
    outTrxns = [] if (csvOut is None) else 0
    prevBlockId = None
    doMerge = mergeTrxnsGroupsByType is not None and getGroupId is not None
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
            tempOutTrxns = mergeTrxnsGroupsByType(trxnsGroups.values())
            if (csvOut is None):
                outTrxns.extend(tempOutTrxns)
            else:
                outTrxns += csvOut.writerows(tempOutTrxns)
            prevBlockId = blockId
            trxnsGroups = cl.OrderedDict()

        groupId = getGroupId(trxn)
        if (groupId not in trxnsGroups)
            trxnsGroups[groupId] = []
        trxnsGroups[groupId].append(trxn)

    if (doMerge):
        tempOutTrxns = mergeTrxnsGroupsByType(trxnsGroups.values())
        if (csvOut is None):
            outTrxns.extend(tempOutTrxns)
        else:
            outTrxns += csvOut.writerows(tempOutTrxns)
    
    return outTrxns 


def getType():

def getOp():

def getOpValue():

def getComment(oldComment, trxnType):
    

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
        - Agrupar transacciones por día para reducir el número.
        - Elegir el orden de los campos de salida.
        - Cambiar el nombre de los campos de salida.
        - Eliminar campos de salida.

        Desde el programa principal se puede elegir los campos de salida que
        se quiera y darle un nombre. A cada campo de salida se le asocia la
        función get que está relacionada con el tipo de campo que es y los 
        campos de entrada necesarios para poder obtener el dato correctamente
        para esa función. La función get a elegir y el tipo de dato de ese
        campo de salida están ligados. La forma de dar al usuario a elegir
        qué tipo de dato de salida es qué campo se hace eligiendo una función
        get entre las posibles.
    """
    # comprobar errores de entrada
    inFileName = sys.argv[1]
    outFileName = sys.argv[2]

    # Los siguientes valores se podrán meter como parámetros al programa, sobre
    # todo al usar interfaz gráfica. Por defecto valores siguientes:
    dateFormat = "%Y-%m-$d %H:%M:%S"
    newDateFormat = "%d-%m-$Y %H:%M:%S"
    newDayFormat = "%d-%m-$Y"
    
    inFieldNames = ["User_ID", "UTC_Time", "Account", "Operation", "Coin", \
            "Change", "Remark"]
    outFieldNames = ["Tipo", "Operacion", "Compra", "MonedaC", "Venta", \
            "MonedaV", "Comision", "MonedaF", "Exchange", "Grupo", \
            "Comentario", "Fecha"]
    outGets = [getType, getOp, getOpValue, getComment]
    outTypes = ["Staking", "Polvo", "Operación", "Deposito", "Retirada"]
    outOps = ["Compra", "Venta", "Comision"]

    outFieldsGetsValues = \
            {outFieldNames[0]: [getType, [inFieldNames[3]]], \
             outFieldNames[1]: [getOp, [inFieldNames[3]]], \
             outFieldNames[2]: [getOpValue, [inFieldNames[3], \
                inFieldNames[5]]], \
             outFieldNames[3]: [None, [inFieldNames[4]]], \
             outFieldNames[4]: [getOpValue, [inFieldNames[3], \
                inFieldNames[5]]], \
             outFieldNames[5]: [None, [inFieldNames[4]]], \
             outFieldNames[6]: [getOpValue, [inFieldNames[3], \
                inFieldNames[5]]], \
             outFieldNames[7]: [None, [inFieldNames[4]]], \
             outFieldNames[8]: [lambda: "Binance", None], \
             outFieldNames[10]: [getComment, [inFieldNames[6], \
                inFieldNames[3]]], \
             outFieldNames[11]: [wrapf(applyDateFormat, dateFormat, \
             newDateFormat), [inFieldNames[1]]]}

    typeMergeKeys = \
            {outTypes[0]: [mergeStakingTrxns, [outFieldNames[3], outFieldNames[2]), [):
    configTrxnBlockId = \
            {"function": lambda v: applyDateFormat(v, dateFormat, dayFormat), \
             "keys": [dateFieldNameOut]}
    configTrxnGroupId = \
            {"staking": [lambda v: getValueWithDate(dateFormat, dayFormat, v),\
                [dateFieldNameOut, typeFieldNameOut, coinFieldNameOut]],

    isCsvInToMem = True
    isCsvOutToMem = True
    
    fileIn = open(fileNameIn, newline='')
    csvIn = csvOpen(fileIn, 'r', isDict=True)
    trxnsIn = [trxn for trxn in csvIn] if isCSVInToMem else csvIn

    csvOut = open(fileNameIn, "w", newline='') if isCsvOutToMem else None
    # Dar antes la opción de agrupar las transacciones itertools groupby 
    outTrxns = csvProcessTrxns(trxnsIn, wrapf(processTrxn,outFieldsGetsValues),\
            csvOut, wrapf(mergeTrxnsGroupsByField, outFieldNames[0], typeMergeKeys),)


    if isCsvOutToMem: 
        csvOut = csvOpen(fileNameOut, 'w', dialect="excel", isDict=True, \
                fieldnames=fieldNamesOut)
        csvOut.writeheader()
        csvWriteRows(csvOut, outTrxns, fieldNamesInOut)

    # cerrar el archivo csv abierto


def csvProcessTrxns(trxnsIn, processTrxn, csvOut=None, \
        mergeTrxnsGroupsByType=None, getGroupId=None, getBlockId=None)

if __name__ in ("__main__", "__console__"):
    main()
