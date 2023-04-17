import json 
import os 
import time 
import tkinter as tk
import re 
import tabulate
from tabulate import*
from PIL import Image, ImageTk

name_data = "data"

#Función que hace el drop de un HBase
def Function_drop(table_name):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if os.path.exists(table_doc):
        with open(table_doc, "r") as f:
            table_data = json.load(f)
        if table_data["state"] == False:
            output.insert('end',f"Table '{table_name}' is disable.")
            return
        os.remove(table_doc)
        output.insert('end',f'Table "{table_name}" dropped')
    else:
        output.insert('end',f'Table "{table_name}" does not exist')

#Funcion para  crear una tabla
def create_table(table_name, columns):
    timestamp = int(time.time()*1000)
    
    column = {}
    for x in columns:
        column[x]={"DATA_BLOCK_ENCODING":"NONE", "BLOOMFILTER":"ROW", "REPLICATION_SCOPE":"0", "VERSIONS":"1", "COMPRESSION":"NONE",}
    
    table_data = {"columns": column, "state":True ,"rows": {}, "created_timestamp":timestamp, "updated_timestamp":timestamp}
    if not os.path.exists(name_data):
        os.makedirs('data')
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if not os.path.exists(table_doc):
        with open(table_doc, "w") as f:
            json.dump(table_data, f)
        
        output.insert('end',f'Table "{table_name}" created with columns "{column}"')
    else:
        
        output.insert('end',f'Table "{table_name}" already exists')

def Fuction_trunc(table_name):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if os.path.exists(table_doc):
        
        with open(table_doc, "r") as f:
            table_data = json.load(f)
            
        if table_data["state"] == True:
            output.insert('end', f"\nTruncating '{table_name}' table  (it may take a while)\n")
            output.insert('end', f"- Disabling table...\n")
            # table_state(table_name,False)
            output.insert('end', f"- Truncating table...\n")
            
            # Leer el archivo JSON
            with open('./data/'+table_name+'.json') as archivo:
                data = json.load(archivo)

            # Extraer las columnas, versiones y replication scopes
            columnas = []
            for nombre_columna, datos_columna in data['columns'].items():
                columna = [nombre_columna, datos_columna['VERSIONS'], datos_columna['REPLICATION_SCOPE']]
                columnas.append(columna)

            # print(columnas)

            Function_drop(table_name)
            
            create_table(table_name, [name[0] for name in columnas])
            
            for column in columnas:
                data = ['VERSIONS', 'REPLICATION_SCOPE']
                value = [column[1],column[2]]
                # print("table_name: ", table_name)
                # print("column: ", column[0])
                # print("data: ",  data)
                # print("value: ", value)
                
                for l in range(len(data)):
                    Fuction_alter_table(table_name,column[0],data[l],value[l])
        # Imprimir el resultado
        # output.insert('end', f"truncating data {columnas}\n")
        else:
            output.insert('end',f"Table '{table_name}' is disable.")
            return
    else:
        output.insert('end',f'Table "{table_name}" does not exist')
#Función para hacer un count 
def Function_count(table_name):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if os.path.exists(table_doc):
        with open(table_doc, "r") as f:
            table_data = json.load(f)
        if table_data["state"] == False:
            output.insert('end',f"Table '{table_name}' is disable.")
            return
        output.insert('end',f'Rows: {len(table_data["rows"])}')
    else:
        output.insert('end',f'Table "{table_name}" does not exist')
        
        
def Fuction_delete(table_name, row, column, time_stamp = None):
    
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if os.path.exists(table_doc):
        
        with open(table_doc, "r") as f:
            table_data = json.load(f)
            
        if table_data["state"] == True:

            with open(table_doc, "r") as f:
                data = json.load(f)
            
            # Extraer la columna y la subcolumna de la celda que se desea eliminar
            column, subcolumn = column.split(':')
            
            # Verificar si la celda que se desea eliminar existe y su marca de tiempo es la correcta
            if row in data['rows'] and column in data['rows'][row] and subcolumn in data['rows'][row][column]:
                if data['rows'][row][column][subcolumn]['timestamp'] == time_stamp or time_stamp == None:
                    # Eliminar la celda
                    del data['rows'][row][column][subcolumn]
                    
                    # Guardar los cambios en el archivo JSON
                    with open(table_doc, 'w') as f:
                        json.dump(data, f)

                    output.insert('end', f"The cell has been successfully deleted.\n")
                else:
                    output.insert('end', f"The cell cannot be deleted because the supplied timestamp does not match the timestamp of the cell.\n")
            else:
                output.insert('end', f"The cell cannot be deleted because it does not exist.\n")
            
            # Leer el archivo JSON y convertirlo en un diccionario
            with open(table_doc, 'r') as f:
                data = json.load(f)

            # Eliminar las propiedades vacías
            for row in data['rows']:
                for prop in list(data['rows'][row]):
                    if not bool(data['rows'][row][prop]):
                        del data['rows'][row][prop]

            # Guardar el resultado en un nuevo archivo JSON
            with open(table_doc, 'w') as f:
                json.dump(data, f)

            # Cargar el archivo JSON
            with open(table_doc, 'r') as f:
                data = json.load(f)

            # Recorrer cada fila en el archivo
            for key in list(data['rows'].keys()):
                # Si la fila no tiene valores, eliminarla
                if not data['rows'][key]:
                    del data['rows'][key]

            # Guardar los cambios en el archivo JSON
            with open(table_doc, 'w') as f:
                json.dump(data, f)

        else:
            output.insert('end',f"Table '{table_name}' is disable.")
            return
    else:
        output.insert('end',f'Table "{table_name}" does not exist')
    


def Delete_all(table_name, row):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if os.path.exists(table_doc):
        
        with open(table_doc, "r") as f:
            table_data = json.load(f)
            
        if table_data["state"] == True:
            
            with open('./data/'+table_name+'.json') as archivo:
                data = json.load(archivo)

            
            if row in data['rows']:
                del data['rows'][row]

            # Escribe el archivo JSON actualizado
            with open('./data/'+table_name+'.json', 'w') as f:
                json.dump(data, f)
        
            output.insert('end',f"From '{table_name}' deleted {row} row")

        else:
            output.insert('end',f"Table '{table_name}' is disable.")
            return
    else:
        output.insert('end',f'Table "{table_name}" does not exist')



#Función para hacer una lista con Hbase
def List_fuction():
    tables = []
    for file in os.listdir(name_data):
        if file.endswith(".json"):
            tables.append(file.replace(".json", ""))
    output.insert('end',f'Tables: {tables}')
    

#Función que hace el drop all de HBase
def drop_all_function(regex):
    table_docs = os.listdir(name_data)
    tables_to_drop = []
    for table_doc in table_docs:
        table_name = table_doc.split('.')[0]
        if re.match(regex, table_name):
            tables_to_drop.append(table_name)
    if not tables_to_drop:
        output.insert('end', f"No tables found matching regex '{regex}'")
        return
    for table_name in tables_to_drop:
        table_doc = os.path.join(name_data, f"{table_name}.json")
        if not os.path.exists(table_doc):
            output.insert('end', f'Table "{table_name}" does not exist')
            continue
        with open(table_doc, "r") as f:
            table_data = json.load(f)
        if not table_data["state"]:
            output.insert('end', f"Table '{table_name}' is disabled and cannot be dropped.")
            return
        os.remove(table_doc)
        output.insert('end', f'Table "{table_name}" dropped')
    
#Función que hace el describe de HBase
def describe_function(table_name):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if os.path.exists(table_doc):
        with open(table_doc, "r") as f:
            table_data = json.load(f)
        if table_data["state"] == False:
            output.insert('end',f"Table '{table_name}' is disable.")
            return
        headers = ["Column Family", "Column", "Version", "Block Encoding", "Compression", "Bloom Filter", "Replication Scope"]
        data = []
        for column in table_data["columns"]:
            temporalArray = []
            # print("Valores de la columna: ", column)
            temporalArray.append(column)
            # data.append([column])
            for key, value in table_data["columns"][column].items():
                # print("key: ", key)
                # print("value: ", value)
                temporalArray.append(value)
                # data.append([key, value])
            data.append(temporalArray)
        # print(data)
        table = tabulate(data, headers=headers, tablefmt="pretty")
        output.insert('end',f'Table "{table_name}"')
        output.insert('end',table)
    else:
        output.insert('end',f'Table "{table_name}" does not exist')

                

#funcion que agrega datos a la tabla 
def put_data(table_name, row_key, column, value):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    if not os.path.exists(table_doc):

        output.insert('end',f"Table '{table_name}' does not exist.")
        return

    with open(table_doc, "r") as f:
        table_data = json.load(f)
    
    if table_data["state"] == False:
        output.insert('end',f"Table '{table_name}' is disable.")
        return

   
    column_family, qualifier = column.split(":")
    if column_family not in table_data["columns"]:
        
        output.insert('end',f"Column '{column}' does not exist in table '{table_name}'.")
        return

    if row_key not in table_data["rows"]:
        table_data["rows"][row_key] = {}

    if column_family not in table_data["rows"][row_key]:
        table_data["rows"][row_key][column_family] = {}
    
   
    timestamp = int(time.time()*1000)
    table_data["updated_timestamp"] = timestamp
    
    
    table_data["rows"][row_key][column_family][qualifier] =  {"value": str(value), "timestamp": timestamp}

    table_data["rows"] = dict(sorted(table_data["rows"].items()))

   
    with open(table_doc, "w") as f:
        json.dump(table_data, f)

   
    output.insert('end',f'Row added to table "{table_name}" with row_key "{row_key}" and column "{column}" set to "{value}"\n')


#para disable o enable la tabla
def table_state(table_name, state):
   
    table_doc = os.path.join(name_data, f"{table_name}.json")

    if not os.path.exists(table_doc):
       
        output.insert('end',f"Table '{table_name}' does not exist.")
        return
    
    with open(table_doc, "r") as f:
        table_data = json.load(f)

    table_data['state'] = state
   
    timestamp = int(time.time()*1000)
    table_data["updated_timestamp"] = timestamp

    with open(table_doc, "w") as f:
        json.dump(table_data, f)

    if state == True:
        text = "enable"
    else:
        text = "disable"

    output.insert('end', f'Table "{table_name}" state is {text}\n')
    
#función alter table 
def Fuction_alter_table(table_name, column, data=[], value=[]):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    timestamp = int(time.time()*1000)
    
    if not os.path.exists(table_doc):
        output.insert('end', f"Table '{table_name}' does not exist.")
        return
    
    with open(table_doc, "r") as f:
        table_data = json.load(f)

   
    if table_data["state"] == False:
        output.insert('end',f"Table '{table_name}' is disable.")
        return
    

    if column not in table_data["columns"]:
        table_data["columns"][column] = {"DATA_BLOCK_ENCODING":"NONE", "BLOOMFILTER":"ROW", "REPLICATION_SCOPE":"0", "VERSIONS":"1", "COMPRESSION":"NONE",}

       
        output.insert('end', f"Column '{column}' has been added in table '{table_name}'.")
    
   
    if "METHOD" == data:
        if value == "delete":   
            for row in table_data['rows'].values():
                if column in row:
                    del row[column]
            del table_data['columns'][column]
            output.insert('end', f"Column '{column}' has been removed of table '{table_name}'.")
            
           
            rowDelete = []
            for testkey in table_data["rows"]:
                if len(table_data["rows"][testkey]) == 0: 
                    rowDelete.append(testkey)
                
            if len(rowDelete) > 0:    
                for l in rowDelete:
                    del table_data["rows"][l]
                
            
            table_data["updated_timestamp"] = timestamp
            
            with open(table_doc, "w") as f:
                json.dump(table_data, f)
            return
        
    if len(data) > 0:
        if data not in table_data["columns"][column]:
            output.insert('end', f"Data '{data}' does not exist in column '{column}' of table '{table_name}'.")
            return
    
        table_data["columns"][column][data] = str(value)
        output.insert('end', f'Table "{table_name}" column "{column}" data "{data}" succesfully changed to "{value}."\n')
    
    table_data["updated_timestamp"] = timestamp
    
    with open(table_doc, "w") as f:
        json.dump(table_data, f)
   
    
    
#Funcion Scan de hbase 

def Fuction_scan(table_name):
    table_doc = os.path.join(name_data, f"{table_name}.json")
    
    if not os.path.exists(table_doc):
        output.insert('end', f"Table '{table_name}' does not exist.")
        return
    
    with open(table_doc, "r") as f:
        table_data = json.load(f)

   
    if table_data["state"] == False:
        output.insert('end',f"Table '{table_name}' is disable.")
        return
        
   
    output.insert('end', "{:<20} {:<30}\n".format("ROW", "COLUMN+CELL"), ('underline'))
  
    for row_key, row_data in table_data['rows'].items():
        for column_family, column_data in row_data.items():
            for qualifier, qualifier_data in column_data.items():
                column = f"{column_family}:{qualifier}"
                value = qualifier_data['value']
                timestamp = qualifier_data['timestamp']
               
                output.insert('end', "{:<20} {:<30}".format(row_key, f"column={column}, timestamp={timestamp}, value={value}\n"))
                
   
    output.tag_configure('underline', underline=True)


def Fuction_get_table(table_name,row,columns):
    table_doc = os.path.join(name_data, f"{table_name}.json")



    if not os.path.exists(table_doc):
       
        output.insert('end', f"Table '{table_name}' does not exist.")
        return
    
    with open(table_doc, "r") as f:
        table_data = json.load(f)

    #revisa su estado si esta disable o enable
    if table_data["state"] == False:
        output.insert('end',f"Table '{table_name}' is disable.")
        return
    
    output.insert('end', "{:<20} {:<30}\n".format("ROW", "CELL"), ('underline'))
    if columns:
        key = []
        value = []
        for x in columns:
            key.append(x.split("=>")[0].replace('"','').replace("'", '').strip())
            value.append(x.split("=>")[1].replace('"','').replace("'", '').strip())

        if "COLUMN" in key:
            indice = key.index("COLUMN")
            data = value.pop(indice).split(":")
            #revisar si existe la columna
            if data[0] not in table_data["rows"][row]:
                output.insert('end', f"Data '{data[0]}' does not exist in table '{table_name}'.")
                return
            column = f"{data[0]}:{data[1]}"
            values = table_data['rows'][row][data[0]][data[1]]['value']
            timestamp = table_data['rows'][row][data[0]][data[1]]['timestamp']
            output.insert('end', "{:<20} {:<30}".format(column, f"timestamp={timestamp}, value={values}\n"))

        else:
            columnas = table_data['columns']
            for key_value, data_value in columnas.items():
                noExiste = False
                for l in range(len(key)):
                    
                    if data_value[key[l]] == value[l]:
                       
                        pass
                    else:
                        noExiste = True
                if noExiste:
                   
                    pass
                else:
                    
                    for row_key, row_data in table_data['rows'][row][key_value].items():

                        column = f"{key_value}:{row_key}"
                        valueAdd = row_data['value']
                        timestamp = row_data['timestamp']
                        # Agregar la información de la celda a la columna COLUMN+CELL
                        output.insert('end', "{:<20} {:<30}".format(column, f"timestamp={timestamp}, value={valueAdd}\n"))

    else:

        if row not in table_data["rows"]:
            output.insert('end', f"Data '{row}' does not exist in table '{table_name}'.")
            return
        

        for row_key, row_data in table_data['rows'][row].items():
           
            for column_family, column_data in row_data.items():
                
                column = f"{row_key}:{column_family}"
                value = column_data['value']
                timestamp = column_data['timestamp']
                output.insert('end', "{:<20} {:<30}".format(column, f"timestamp={timestamp}, value={value}\n"))

        
    output.tag_configure('underline', underline=True)


def ViewText():
    Clear_text()
    command = entry.get()
    

    command = re.sub(r"{\s*(\w+)\s*=>", r"{\g<1>=>", command)
    
 
    split_parts = re.findall(r"[^,{}]+(?:\{[^{}]*\})*", command)

    command_parts = [part.strip() for part in split_parts if part.strip()]

    command_parts = [part for part in command_parts if bool(part)]
    

    if command.startswith('create'):

        table_name = command_parts[0].replace('create ', '').replace('"', '').replace("'", '').strip()
        columns = [col.replace('"', '').replace("'", '').strip() for col in command_parts[1:]]

        create_table(table_name, columns)
        
    elif command.startswith('put'):
   
        table_name = command_parts[0].replace('put ', '').replace('"', '').replace("'", '').strip()

        i = 1
        array_row_key = []
        seguir = True
        while(seguir):
            if i < len(command_parts):
                array_row_key.append(command_parts[i].replace('"', '').replace("'", '').strip())
                i += 3
            else:
                seguir=False
        

        i = 2
        array_column = []
        seguir = True
        while(seguir):
            if i < len(command_parts):
                array_column.append(command_parts[i].replace('"', '').replace("'", '').strip())
                i += 3
            else:
                seguir=False

        i = 3
        array_value = []
        seguir = True
        while(seguir):
            if i < len(command_parts):
                array_value.append(command_parts[i].replace('"', '').replace("'", '').strip())
                i += 3
            else:
                seguir=False

        rowKeyLen = len(array_row_key)
        columLen = len(array_column)
        valueLen = len(array_value)


        if rowKeyLen == columLen == valueLen:
            for l in range(rowKeyLen):
                put_data(table_name,array_row_key[l],array_column[l],array_value[l])
            

        else:
            output.insert('end',f'Error, falta algun argumento')

        

    elif command.startswith('disable'):
        table_name = command.replace("disable", '').replace('"','').replace("'", '').strip()
        table_state(table_name,False)

    elif command.startswith('enable') or command.startswith('Is_Enable'):
        table_name = command.replace("enable", '').replace("Is_Enable", '').replace('"','').replace("'", '').strip()
        table_state(table_name,True)

    elif command.startswith('alter'):
        table_name = command_parts[0].replace("alter", '').replace('"','').replace("'", '').replace(",", '').strip()
        columns = command_parts[1].replace("{", '').replace("}","").replace('"','').replace("'", '').split(",")
        parts = columns.pop(0).split("=>")
        column = parts[1].replace('"','').replace("'", '').strip()
        data = []
        value = []
        for l in range(len(columns)):
            parts = columns[l].split("=>")
            data.append(parts[0].replace('"','').replace("'", '').strip())
            value.append(parts[1].replace('"','').replace("'", '').strip())
        
        if len(data) > 0:
            for l in range(len(data)):
                Fuction_alter_table(table_name,column,data[l],value[l])
        else:
            Fuction_alter_table(table_name,column)
        

    elif command.startswith("scan"):
        table_name = command.replace("scan", '').replace('"','').replace("'", '').strip()
        Fuction_scan(table_name)
    

    elif command.startswith("drop_all"):
        regex = command.replace("drop_all", '').replace('"','').replace("'", '').strip()
        drop_all_function(regex)

    elif command.startswith("drop"):
        table_name = command.replace("drop", '').replace('"','').replace("'", '').strip()
        Function_drop(table_name)

    elif command.startswith("describe"):
        table_name = command.replace("describe", '').replace('"','').replace("'", '').strip()
        describe_function(table_name)
    

    elif command.startswith("list"):
        List_fuction()
        

    elif command.startswith("truncate"):
        table_name = command.replace("truncate", '').replace('"','').replace("'", '').strip()

        Fuction_trunc(table_name)
        

    elif command.startswith("deleteall"):
        
        table_name = command_parts[0].replace('deleteall', '').replace('"', '').replace("'", '').strip()
        columns = [col.replace('"', '').replace("'", '').strip() for col in command_parts[1:]]
        Delete_all(table_name,columns[0])
        
    elif command.startswith("delete"):
        
        table_name = command_parts[0].replace('delete', '').replace('"', '').replace("'", '').strip()
        columns = [col.replace('"', '').replace("'", '').strip() for col in command_parts[1:]]
        
        if len(columns) > 2:
            Fuction_delete(table_name,columns[0],columns[1] ,int(columns[2]))
        else:
            Fuction_delete(table_name,columns[0],columns[1])
        
    #get command
    elif command.startswith("get"):
        table_name = ""
        row = ""
        columns = ""
        table_name = command_parts[0].replace("get", '').replace('"','').replace("'", '').replace(",", '').strip()
        if len(command_parts) == 2:
            row = command_parts[1].replace('"','').replace("'", '').strip()

            Fuction_get_table(table_name,row,columns)
        elif len(command_parts) == 3:
            row = command_parts[1].replace('"','').replace("'", '').strip()
            columns = command_parts[2].replace("{","").replace("}","").split(",")

            Fuction_get_table(table_name,row,columns)
        else:
            output.insert("Error, Missing Values")
  
    elif command.startswith("count"):
        table_name = command.replace("count", '').replace('"','').replace("'", '').strip()
        Function_count(table_name)
  
    else:
        output.insert('end',"Comando no reconocido")
    

def Clear_text():
    output.delete('1.0', tk.END)



# Generar la ventana principal
root = tk.Tk()
root.title("ProyectoHbase")

# Crear un marco para el título y el logo
title_frame = tk.Frame(root)
title_frame.pack(side=tk.TOP, pady=10)

    # Cargar la imagen para el botón
button_image = Image.open("button_bg.png")
button_photo = ImageTk.PhotoImage(button_image)
button_image_resized = button_image.resize((100, 40))
button_photo = ImageTk.PhotoImage(button_image_resized)


# Agregar el logo de HBase
logo_image = tk.PhotoImage(file="hbase_logo.png")
logo_label = tk.Label(title_frame, image=logo_image)
logo_label.image = logo_image
logo_label.pack(side=tk.RIGHT, padx=10)

# Crear un cuadro de texto
entry = tk.Entry(root, width=100)
entry.pack()


# Crear un botón de submit
button_font = ("Helvetica", 14, "bold")
submit_button = tk.Button(root, text="Submit", font=button_font, fg="white", borderwidth=0, image=button_photo, compound="center", highlightthickness=0, command=ViewText)
submit_button.image = button_photo
submit_button.config(height=30, width=100)
submit_button.pack(pady=10)


# Crear un cuadro de texto para la salida
output = tk.Text(root, height=50, width=150)
output.pack()

root.mainloop()
