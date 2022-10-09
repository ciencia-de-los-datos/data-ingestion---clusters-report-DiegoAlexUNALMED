"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    with open('clusters_report.txt',mode = 'r') as archivo:
        #Lectura de texto
        texto = archivo.readlines()

        #Extracción de campos
        campos1 = texto[0].strip()
        campos2 = texto[1].strip()
        campos = [campos1[0:7],campos1[9:21]+campos2[0:14],campos1[25:39]+campos2[16:30],campos1[41:67]]
        campos = list(map(lambda x: x.lower().replace(" ","_"),campos))

        #Procesar la entrada de la información a data estructurada.

        entrada = list(map(lambda x: x.strip(),texto[4:]))
        entrada = [x for x in entrada if x != ""]

        entrada_procesada = []

        for i in range(13):
            j = i + 1
            fila = []
            if j in (1, 4, 5, 6, 7, 8, 9, 10, 13):
                if j < 10:
                    for k in range(len(entrada)):

                        if entrada[k][0] == str(j):
                            fila = [entrada[k][0], entrada[k][5:9], entrada[k][22:26], entrada[k][38:]+" "+entrada[k+1]+" "+entrada[k+2]+" "+ entrada[k+3]]
                            break
                else:
                    for k in range(len(entrada)):

                        if entrada[k][0:2] == str(j):
                            fila = [entrada[k][0:2], entrada[k][5:9], entrada[k][22:26], entrada[k][38:]+" "+entrada[k+1]+" "+entrada[k+2]+" "+ entrada[k+3]]
                            break
            elif j in (2,12):
                if j == 2:
                    for k in range(len(entrada)):
                        if entrada[k][0] == str(j):
                            fila = [entrada[k][0], entrada[k][5:9], entrada[k][22:26], entrada[k][38:]+" "+entrada[k+1]+" "+entrada[k+2]+" "+ entrada[k+3]+" "+ entrada[k+4]]
                            break
                else:
                    for k in range(len(entrada)):
                        if entrada[k][0:2] == str(j):
                            fila = [entrada[k][0:2], entrada[k][5:9], entrada[k][22:26], entrada[k][38:]+" "+entrada[k+1]+" "+entrada[k+2]+" "+ entrada[k+3]+" "+ entrada[k+4]]
                            break
            elif j == 3:
                for k in range(len(entrada)):
                    if entrada[k][0] == str(j):
                        fila = [entrada[k][0], entrada[k][5:9], entrada[k][22:26], entrada[k][38:]+" "+entrada[k+1]+" "+entrada[k+2]]
                        break
            elif j == 11:
                for k in range(len(entrada)):
                    if entrada[k][0:2] == str(j):
                        fila = [entrada[k][0:2], entrada[k][5:9], entrada[k][22:26], entrada[k][38:]+" "+entrada[k+1]]
                        break
            entrada_procesada.append(fila)
    df = pd.DataFrame(entrada_procesada, columns = campos)
    df["cluster"] = df["cluster"].astype("int32")
    return df
