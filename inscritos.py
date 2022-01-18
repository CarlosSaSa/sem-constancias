import mysql.connector
import pandas as pd
import numpy as np
import re
from math import isnan
from datetime import date


class datos:
    """ CONSTRUCTOR DE LA CLASE EN DONDE LOS PARAMETROS PASADOS SON LOS ATRIBUTOS DE LA CLASE"""

    def __init__(self, consulta, archivo, hoja):
        self.consulta = consulta
        self.archivo = archivo
        self.hoja = hoja

    def insercion_registros(self, consulta):
        mydb = mysql.connector.connect(
            host="*********",
            user="juanCARLOS",
            passwd="***************",
            database="CENTRALconstancias"
        )
        cursor = mydb.cursor()
        cursor.execute(consulta)
        lista = []
        for i in cursor.fetchall():
            lista.append("".join(i))

        return lista

    def insercion_registros_bd(self, archivo, hoja, lista):
        mydb = mysql.connector.connect(
            host="*************",
            user="juanCARLOS",
            passwd="***********",
            database="CENTRALconstancias"
        )
        cursor = mydb.cursor()
        df = pd.read_excel(archivo, sheet_name=hoja, usecols=list(np.arange(1, 11, 1)))
        expresion = "[A-Z]{4}[0-9]{6}[A-Z]{6}[0-9]{2}"
        patron = re.compile(expresion)
        z = 0
        curps_cns_dep = []
        aux = []
        today = str(date.today())
        f = open("Registrados_no_aceptados_inscritos.txt", "w")
        #FILTRANDO LOS CURPS
        for i in lista:
            try:
                aux.append(patron.match(i).group())
            except:
                #print("CURP NO ADMITIDO {}".format(i))
                pass

        for i in aux:
            for y in df.dropna(subset = ['CURP']).itertuples():
                if i == str(getattr(y, "CURP")).rstrip().lstrip():
                    z+=1
                    per_curp = str(getattr(y, "CURP").rstrip().lstrip())
                    per_apellido = str(getattr(y, "_2").rstrip().lstrip())
                    per_nombre = str(getattr(y, "Nombre").rstrip().lstrip())
                    per_genero = str(getattr(y, "Género").rstrip().lstrip())
                    per_tipo = int(getattr(y, "Tipo"))
                    per_correo = str(getattr(y, "correo").rstrip().lstrip())

                    if isnan(float(getattr(y, "_7"))):
                        f.write("Datos mal formateados para CURP: {} en la columna institucion con valor: {} \n".format(getattr(y, "CURP"),getattr(y, "_7")))
                        institucion = "NULL"
                    else:
                        institucion = int(getattr(y, "_7"))

                    if isnan(float(getattr(y, "Sede"))):
                        f.write("Datos mal formateados para CURP: {} en la columna Sede con valor: {} \n".format(getattr(y, "CURP"), getattr(y, "Sede")))
                        sede = "NULL"
                    else:
                        sede = int(getattr(y, "Sede"))

                    if isinstance(getattr(y, "Especialidad"), str) or isnan(float(getattr(y, "Especialidad"))) :
                        f.write("Datos mal formateados para CURP: {} en la columna especialidad con valor: {} \n".format(getattr(y, "CURP"),getattr(y, "Especialidad")))
                        especialidad = "NULL"
                    else:
                        especialidad = int(getattr(y, "Especialidad"))

                    if isinstance(getattr(y, "_10"), str):
                        f.write("Datos mal formateados para CURP: {} en la columna erre con valor: {} \n".format(getattr(y, "CURP"), getattr(y, "_10") ))
                        erre = "NULL"
                    else:
                        erre = int(getattr(y, "_10"))

                    print( "Insertando registro CURP N° {} : {} Apellido: {} Nombre: {} Genero: {} Tipo: {} Correo: {} Institucion: {} Sede: {} Especialidad: {} Año de registro: {} Fecha de registro: {}".format(z, per_curp, per_apellido, per_nombre, per_genero, per_tipo, per_correo, institucion, sede, especialidad, erre, today))
                    sql = "INSERT INTO persona(per_curp, per_apellido, per_nombre, per_genero, per_tipo, per_correo, institucion, sede, especialidad, erre, per_fecha_registro) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    values = (per_curp, per_apellido, per_nombre, per_genero, per_tipo, per_correo, institucion, sede, especialidad, erre, today)
                    cursor.execute(sql, values)
                    mydb.commit()
        mydb.close()


if __name__ == "__main__":
    consulta = "SELECT per_curp FROM cns_dep WHERE per_curp not in (select per_curp from persona)"
    archivo = "NUEVOS INCRITOS MANUAL.xlsx"
    hoja = "RESIDENTES"
    obj = datos(consulta, archivo, hoja)
    obj.insercion_registros_bd(archivo, hoja, obj.insercion_registros(consulta))
