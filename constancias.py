import mysql.connector
import pandas as pd
import numpy as np
import re
from datetime import date

class datos:
    """ CONSTRUCTOR DE LA CLASE EN DONDE LOS PARAMETROS PASADOS SON LOS ATRIBUTOS DE LA CLASE"""

    def __init__(self, consulta, archivo, hoja):
        self.consulta = consulta
        self.archivo = archivo
        self.hoja = hoja

    """METODO DONDE ESTABLEZCO LA CONEXION A LA BASE DE DATOS, CREO UN CURSOR CON EL METODO CURSOR,
    EJECUTO LA CONSULTA CON EL METODO EXECUTE, FINALMENTE RECORRO LA CONSULTA CON EL CURSOR CON EL
     METODO FETCHALL, COMO AL RECORRER LA TABLA SE DEVUELVE UNA TUPLA POR REGISTRO ENTONCES CONVIERTO
     LA TUPLA CON EL METODO JOIN  A UN STRING Y LO AGREGO A UNA LISTA, FINALMENTE REGRESO LA LISTA"""

    def insercion_registros(self, consulta):
        mydb = mysql.connector.connect(
            host="***********",
            user="juanCARLOS",
            passwd="**********",
            database="CENTRALconstancias"
        )
        cursor = mydb.cursor()
        cursor.execute(consulta)
        lista = []
        for i in cursor.fetchall():
            lista.append("".join(i))
        return lista

    """ LOS PARAMETROS PASADOS SON EL NOMBRE DEL ARCHIVO Y EL NOMBRE DE LA HOJA
    CON EL METODO READ_EXCEL LEO EL ARCHIVO EXCEL A PARTIR DE LA COLUMNA 1 HASTA LA 9
    ESTABLEZCO UNA EXPRESION REGULAR Y LA COMPILO CON EL METODO COMPILE
    CON EL METODO DROPNA ELIMINO AQUELLOS REGISTROS DONDE EL VALOR ES NAN O NULO
    CON EL METODO ITERTUPLES RECORRO CADA REGISTRO DEL EXCEL
    CON EL METODO GETATTR SOLO CAPTURO LOS REGISTROS EN DONDE LA COLUMNA SEA EL CURP
    FINALMENTE CON CADA CURP OBTENIDO SE AGREGA A LA LISTA EN FORMA DE STRING
    DESPUES RECORRO LA LISTA DONDE SE ALMACENARON LOS CURPS Y POR MEDIO DE LA EXPRESION REGULAR
    FILTRO AQUELLOS CURPS QUE NO COINCIDAN CON DICHA EXPRESION REGULAR Y LOS QUE COINCIDEN 
    SE AGREGAN A LA LISTA CURPS, SI SE ENCUENTRAN CURPS NO COINCIDENTES SE IGNORA EL PASO
    """

    def leer_excel(self, archivo, hoja):
        df = pd.read_excel(archivo, sheet_name=hoja, usecols=list(np.arange(1, 10, 1))).drop(['FECHA NEW'], axis=1)
        expresion = "[A-Z]{4}[0-9]{6}[A-Z]{6}[0-9]{2}"
        patron = re.compile(expresion)
        aux = []
        curps = []
        f = open("registros_no_aceptados.txt","w")

        for y in df.dropna().itertuples():
            aux.append(str(getattr(y, "CURP")))

        for i in aux:
            try:
                curps.append(patron.match(i).group())
            except:
                #print("CURP NO ADMITIDO {}".format(i))
                f.write("Curp no admitido: {} \n".format(i))
                pass
        f.close()
        return list(dict.fromkeys(curps))
        #Regresa una lista sin elementos duplicados

    """ EN ESTE METODO SE PASAN DOS LISTAS LAS CUALES TIENEN QUE SER LA LISTA DE LOS CURPS EXTRAIDOS DEL EXCEL Y LOS CURPS
    EXTRADOS DE LA BASE DE DATOS, SE REALIZA UN CICLO EN LA LISTA QUE CONTIENE LOS CURPS DEL ARCHIVO DE EXCEL,
    SI CADA ELEMENTO DE ESTA LISTA NO SE ENCUENTRA EN LA LISTA QUE CONTIENE LOS CURPS DE LA BASE DE DATOS SE DEVUELVE 
    EL MENSAJE DE QUE EL CURP NO SE ENCUENTRA 
    EL METODO RSTRIP() Y LSTRIP() ELIMINAN LOS ESPACIOS EN BLANCO A LA IZQUIERDA Y A LA DERECHA DE LA CADENA."""

    def comprobacion(self, l1, l2):
        no_existe = []
        for i in l1:
            if i.rstrip().lstrip() not in l2[1:]:
                no_existe.append(i.rstrip().lstrip())
        return no_existe

    def insercion_registros_bd(self, lista):
        mydb = mysql.connector.connect(
            host="132.248.250.229",
            user="juanCARLOS",
            passwd="N0B0RR3ZN4d4",
            database="CENTRALconstancias"
        )
        cursor = mydb.cursor()
        df = pd.read_excel(archivo, sheet_name=hoja, usecols=list(np.arange(1, 10, 1)))
        z=0
        f = open("registros_insertados.txt", "w")
        today = str(date.today())
        for i in lista:
            for y in df.dropna().itertuples():
                if i == str(getattr(y, "CURP")):
                    z+=1
                    per_curp = str(getattr(y, "CURP").rstrip().lstrip())
                    id_curso = int(getattr(y, "_2"))
                    folio = str(getattr(y, "REGISTRO").rstrip().lstrip())
                    imparticion_anterior = str(getattr(y, "FECHA").rstrip().lstrip())
                    imparticion = str(getattr(y, "_5")).rstrip().lstrip()
                    ano = int(getattr(y, "AÑO"))
                    fecha_registro = str(getattr(y, "_8"))
                    aula = str(getattr(y, "AULA"))

                    if aula == '2.0':
                        aula = int('20')
                    else:
                        aula = int(aula)

                    print("CURP NO {} {} id_curso: {} folio: {} año: {} imparticion anterior: {} imparticion: {} fecha_registro: {} aula: {} bloqueo: {} regtem: {}".format(z, per_curp, id_curso, folio, ano, imparticion_anterior, imparticion, fecha_registro, aula, "null", today))
                    f.write("CURP NO {} {} id_curso: {} folio: {} año: {} imparticion anterior: {} imparticion: {} fecha_registro: {} aula: {} bloqueo: {} regtem: {} \n".format(z, per_curp, id_curso, folio, ano, imparticion_anterior, imparticion, fecha_registro, aula, "null", today))
                    sql = "INSERT INTO cns_dep (id_curso, per_curp, folio, imparticion, imparticion_anterior, ano, fecha_registro,aula,bloqueo,regtem) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    values = (id_curso, per_curp, folio, imparticion, imparticion_anterior, ano, fecha_registro, aula, "null", today)
                    cursor.execute(sql, values)
                    mydb.commit()
        mydb.close()


if __name__ == "__main__":
    consulta = "SELECT per_curp FROM cns_dep"
    archivo = "Registro de Constancias SEMEDU.xlsx"
    hoja = "NewResidentes"
    obj = datos(consulta, archivo, hoja)
    obj.insercion_registros_bd(obj.comprobacion(obj.leer_excel(archivo, hoja), obj.insercion_registros(consulta)))
