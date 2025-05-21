import psycopg2
import streamlit as st
import base64
import io
from PIL import Image
import streamlit as st
import pandas as pd
import requests
from io import BytesIO

def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()  # para avisar si falla la descarga
    img = Image.open(BytesIO(response.content))
    return img

# URLs directas que compartiste
logo_url = "https://i.postimg.cc/RZyrJ6r2/logo.png"
workload_logo_url = "https://i.postimg.cc/4NwsyxxT/workload-logo-hd.png"

# Cargar y mostrar logo principal
logo_image = load_image_from_url(logo_url)
st.image(logo_image, width=200)



# Mostrar empresa en el sidebar si está logueado
if st.session_state.get("logueado"):
    st.sidebar.markdown(
        f"""
        <div style='background-color: #f0f2f6; padding: 8px 12px; border-radius: 8px; 
                    font-weight: bold; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);'>
            🏢 {st.session_state.empresa}
        </div>
        """,
        unsafe_allow_html=True
    )

# Mostrar logo en el sidebar (usando la imagen cargada)
logo_path = "workload_logo_hd.png"
logo_image = Image.open(logo_path)
st.sidebar.image(logo_image, width=150)


import psycopg2
import streamlit as st

DATABASE_URL = "postgresql://postgres:eqWOTMrsVejNlRKwcNhvPiRbXRyKYyKM@nozomi.proxy.rlwy.net:11260/railway"

# Lista de esquemas válidos
ESQUEMAS_VALIDOS = {"public", "empresa1", "empresa2", "empresa3", "empresa4", "empresa5"}

def get_connection(empresa):
    try:
        if empresa not in ESQUEMAS_VALIDOS:
            st.error(f"Esquema '{empresa}' no es válido. Esquemas válidos: {', '.join(ESQUEMAS_VALIDOS)}")
            return None
        # Conectar usando la URL de Railway
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        # Cambiar al esquema de la empresa
        cursor.execute(f"SET search_path TO {empresa};")
        # Verificar el esquema activo
        cursor.execute("SHOW search_path;")
        esquema_activo = cursor.fetchone()
        print(f"Esquema activo: {esquema_activo}")
        return conn
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

# Ejemplo de uso:
if __name__ == "__main__":
    conn = get_connection("empresa1")
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT version();")
            print(cur.fetchone())
            cur.close()
        except Exception as e:
            print(f"Error ejecutando consulta: {e}")
        finally:
            conn.close()
    else:
        print("No se pudo conectar a la base de datos.")
def home():
    st.title("Bienvenido a la Aplicación de Cargas de Trabajo")
    st.write("""
    Workload ofrece un seguimiento eficiente y en tiempo real de las asignaciones laborales, facilitando la gestión de tareas y aumentando la productividad en distintos departamentos o equipos. Cuenta con una interfaz intuitiva que permite monitorear cargas de trabajo, asignar responsabilidades y optimizar la distribución de tareas, garantizando así una operación más organizada y eficaz.
    """)

#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

# Función para ver la carga de trabajo de un funcionario por su ID
def ver_carga_trabajo():
    # Ingreso del ID del funcionario
    user_id = st.text_input("ID del funcionario:")

    # Botón para realizar la búsqueda
    if st.button("Buscar Carga de Trabajo"):
        if user_id:
            # Obtener el esquema dinámico (tenant)
            tenant = st.session_state.get('empresa', 'public')  # Si no está definido, se usa 'public' por defecto

            # Conectar a la base de datos
            conexion = get_connection(tenant)  # Cambio aquí para usar el esquema dinámico
            if conexion:
                cursor = conexion.cursor()
                try:
                    # Consulta para obtener la carga de trabajo con el esquema dinámico
                    cursor.execute(f"""
                        SELECT funcionario_id, carga_total_trabajo 
                        FROM {tenant}.tb_carga_trabajo 
                        WHERE funcionario_id = %s
                    """, (user_id,))
                    row = cursor.fetchone()

                    if row:
                        # Si se encuentra el registro, mostrarlo con mejor visibilidad
                        funcionario_id, carga_total_trabajo = row
                        resultado = f"Carga de trabajo del funcionario con ID {funcionario_id} es: {carga_total_trabajo} %"

                        # Usar st.success, pero con un mejor contraste
                        st.markdown(f'<p style="color: black; font-size: 18px; font-weight: bold;">{resultado}</p>', unsafe_allow_html=True)

                        # Registrar la acción en el historial de modificaciones
                        nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                        empresa = st.session_state.get('empresa', 'Desconocida')
                        accion = f"Vió la carga de trabajo del funcionario con ID {funcionario_id}"

                        # Insertar en la tabla de historial de modificaciones
                        cursor.execute(f"""
                            INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                            VALUES (%s, %s, %s, %s)
                        """, (user_id, nombre_usuario, empresa, accion))
                        conexion.commit()  # Confirmar los cambios
                    else:
                        st.info("No se encontraron registros para este ID.")
                except Exception as e:
                    st.error(f"Error al consultar la base de datos: {e}")
                finally:
                    cursor.close()
                    conexion.close()


# Función ver todas las cargas de trabajo
def ver_todas_cargas_trabajo():
  
    def mostrar_cargas(orden="ASC"):
        tenant = st.session_state.get('empresa', 'public')
        
        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                cursor.execute(f"""
                    SELECT 
                        ct.funcionario_id, 
                        f."Nombre", 
                        ct.carga_total_trabajo, 
                        ct.horas_trabajo, 
                        ct.tiempo_laborado 
                    FROM {tenant}.tb_carga_trabajo ct
                    JOIN {tenant}.tb_funcionarios f ON ct.funcionario_id = f."Id"
                    ORDER BY ct.funcionario_id {orden}
                """)
                rows = cursor.fetchall()

                if rows:
                    df = pd.DataFrame(rows, columns=[ 
                        "Identificación del funcionario",
                        "Nombre del funcionario",
                        "Carga total de trabajo (%)",
                        "Horas de trabajo",
                        "Tiempo laborado"
                    ])

                    st.dataframe(df.style.set_properties(**{
                        'text-align': 'left',
                        'white-space': 'nowrap'
                    }), use_container_width=True)

                    # Botón de descarga en formato CSV
                    csv = df.to_csv(index=False).encode('utf-8')
                    if st.download_button("📥 Descargar como CSV", csv, "cargas_trabajo.csv", "text/csv"):
                        # Registro en historial cuando se descarga CSV
                        nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                        empresa = st.session_state.get('empresa', 'Desconocida')
                        accion = "Descargó las cargas de trabajo como CSV"

                        # Insertar en el historial
                        cursor.execute(f"""
                            INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                            VALUES (%s, %s, %s, %s)
                        """, (nombre_usuario, nombre_usuario, empresa, accion))
                        conexion.commit()

                    # Botón de descarga en formato Excel
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df.to_excel(writer, index=False, sheet_name='CargasTrabajo')
                        worksheet = writer.sheets['CargasTrabajo']
                        for i, column in enumerate(df.columns):
                            column_width = max(df[column].astype(str).map(len).max(), len(column)) + 5
                            worksheet.set_column(i, i, column_width)
                    output.seek(0)
                    excel_data = output.getvalue()
                    if st.download_button(
                        "📥 Descargar como Excel",
                        data=excel_data,
                        file_name='cargas_trabajo.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    ):
                        # Registro en historial cuando se descarga Excel
                        nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                        empresa = st.session_state.get('empresa', 'Desconocida')
                        accion = "Descargó las cargas de trabajo como Excel"

                        # Insertar en el historial
                        cursor.execute(f"""
                            INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                            VALUES (%s, %s, %s, %s)
                        """, (nombre_usuario, nombre_usuario, empresa, accion))
                        conexion.commit()

                else:
                    st.info("No se encontraron registros.")
                
                # Registrar la acción de ver todas las cargas de trabajo
                nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                empresa = st.session_state.get('empresa', 'Desconocida')
                accion = "Vió todas las cargas de trabajo de los funcionarios"

                # Insertar en la tabla de historial de modificaciones
                cursor.execute(f"""
                    INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                    VALUES (%s, %s, %s, %s)
                """, (nombre_usuario, nombre_usuario, empresa, accion))
                conexion.commit()  # Confirmar los cambios

            except Exception as e:
                st.error(f"Error al consultar la base de datos: {e}")
            finally:
                cursor.close()
                conexion.close()

    st.title("Ver Todas las Cargas de Trabajo")

    orden = st.radio("Ordenar por ID del funcionario:", ["Ascendente", "Descendente"], horizontal=True)

    if orden == "Ascendente":
        mostrar_cargas("ASC")
    else:
        mostrar_cargas("DESC")


from datetime import datetime

# Función para eliminar un funcionario y sus actividades
from datetime import datetime

# Función para eliminar un funcionario de todas las tablas
def eliminar_funcionario():
    tenant = st.session_state.get('empresa')  # Esquema activo

    if not tenant:
        st.error("No se ha definido el esquema de la empresa.")
        return

    funcionario_id = st.text_input("Ingrese el ID del funcionario a eliminar")

    if st.button("Eliminar Funcionario"):
        if not funcionario_id.isdigit():
            st.warning("El ID debe ser un número.")
            return

        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                # Verificamos si el ID existe en alguna tabla relevante
                tablas = ["tb_funcionarios", "tb_usuarios", "tb_actividades", "tb_carga_trabajo"]
                existe = False
                for tabla in tablas:
                    columna = "\"Id\"" if tabla == "tb_funcionarios" else "\"id\"" if tabla == "tb_usuarios" else "\"funcionario_id\"" if tabla == "tb_carga_trabajo" else "\"id_funcionario\""
                    cursor.execute(f"SELECT 1 FROM {tenant}.{tabla} WHERE {columna} = %s LIMIT 1", (funcionario_id,))
                    if cursor.fetchone():
                        existe = True
                        break

                if not existe:
                    st.warning(f"No se encontró ningún registro con ID {funcionario_id} en las tablas relacionadas.")
                    return

                # Eliminación segura en todas las tablas
                cursor.execute(f"DELETE FROM {tenant}.tb_carga_trabajo WHERE \"funcionario_id\" = %s", (funcionario_id,))
                cursor.execute(f"DELETE FROM {tenant}.tb_actividades WHERE \"id_funcionario\" = %s", (funcionario_id,))
                cursor.execute(f"DELETE FROM {tenant}.tb_funcionarios WHERE \"Id\" = %s", (funcionario_id,))
                cursor.execute(f"DELETE FROM {tenant}.tb_usuarios WHERE \"id\" = %s", (funcionario_id,))

                # Insertar en historial
                nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                empresa = tenant
                accion = f"Eliminó al funcionario con ID {funcionario_id} de todas las tablas"

                cursor.execute(f"""
                    INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                    VALUES (%s, %s, %s, %s)
                """, (nombre_usuario, nombre_usuario, empresa, accion))

                conexion.commit()
                st.success(f"Funcionario con ID {funcionario_id} eliminado correctamente de todas las tablas.")
            except Exception as e:
                st.error(f"Error al eliminar funcionario: {e}")
            finally:
                cursor.close()
                conexion.close()




# Función para agregar un nuevo funcionario
def agregar_funcionario():
    # Campo de entrada para el ID del funcionario
    funcionario_id = st.text_input("Id del funcionario")
    
    # Verificación de empresa (tenant)
    tenant = st.session_state.get('empresa', 'public')
    if not tenant:
        st.error("No se ha seleccionado ninguna empresa.")
        return
    
    nombre = ""
    if funcionario_id:
        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                cursor.execute(f"SELECT usuario FROM {tenant}.tb_usuarios WHERE id = %s", (funcionario_id,))
                row = cursor.fetchone()
                if row:
                    nombre = row[0]
                    st.success(f"Nombre del funcionario: {nombre}")
                else:
                    st.warning("No se encontró un usuario con ese ID.")
            except Exception as e:
                st.error(f"Error al consultar el nombre: {e}")
            finally:
                cursor.close()
                conexion.close()

    # Resto de campos
    dependencia = st.text_input("Dependencia")
    puesto = st.text_input("Puesto")
    jornada = st.number_input("Jornada (Horas/día)", min_value=0.0)
    feriados = st.number_input("Feriados (Días en el período de estudio)", min_value=0.0)
    horas_extra = st.number_input("Horas extra (Total de horas extra en el período de estudio)", min_value=0.0)
    vacaciones = st.number_input("Vacaciones (Total de días disfrutados en el período de estudio)", min_value=0.0)
    incapacidades = st.number_input("Incapacidades (Días de incapacidad sin considerar días no hábiles)", min_value=0.0)
    permiso = st.number_input("Permiso (Días hábiles solicitados)", min_value=0.0)
    comentarios = st.text_area("Comentarios")

    # Botón para agregar el funcionario
    if st.button("Agregar Funcionario"):
        # Validaciones básicas
        if not funcionario_id.isdigit():
            st.warning("El Id debe ser un número.")
            return
        if not nombre or not dependencia or not puesto:
            st.warning("Los campos Dependencia y Puesto son obligatorios, y el Id debe estar registrado en tb_usuarios.")
            return

        # Cálculos de horas
        dias_laborales = 260
        total_laborable_base = jornada * dias_laborales
        horas_no_laborables = (vacaciones + feriados + incapacidades + permiso) * jornada
        total_laborable = total_laborable_base + horas_extra - horas_no_laborables

        st.write(f"Total Laborable Final: {total_laborable}")

        # Conectar a la base de datos
        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                # Verificar si el funcionario ya existe
                cursor.execute(f"SELECT * FROM {tenant}.tb_funcionarios WHERE \"Id\" = %s", (funcionario_id,))
                if cursor.fetchone():
                    st.warning(f"El Id {funcionario_id} ya está en uso.")
                    return

                # Insertar en tb_funcionarios
                cursor.execute(
                    f"""INSERT INTO {tenant}.tb_funcionarios 
                    ("Id", "Nombre", "Dependencia", "Puesto", "Jornada", "Feriados", "Horas_extra", 
                    "Vacaciones", "Incapacidades", "Permiso", "Otro/Comentarios")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (funcionario_id, nombre, dependencia, puesto, jornada, feriados, horas_extra, 
                     vacaciones, incapacidades, permiso, comentarios)
                )

                # Insertar o actualizar en tb_carga_trabajo
                cursor.execute(f"SELECT * FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (funcionario_id,))
                if cursor.fetchone():
                    cursor.execute(
                        f"""
                        UPDATE {tenant}.tb_carga_trabajo 
                        SET horas_trabajo = %s
                        WHERE funcionario_id = %s
                        """,
                        (total_laborable, funcionario_id)
                    )
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO {tenant}.tb_carga_trabajo (funcionario_id, horas_trabajo)
                        VALUES (%s, %s)
                        """,
                        (funcionario_id, total_laborable)
                    )

                # Calcular y actualizar carga total de trabajo
                cursor.execute(f"SELECT tiempo_laborado, horas_trabajo FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (funcionario_id,))
                row = cursor.fetchone()
                if row:
                    tiempo_laborado = row[0] if row[0] else 0
                    horas_trabajo = row[1] if row[1] else 1
                    if horas_trabajo > 0:
                        carga_total_trabajo = (tiempo_laborado / horas_trabajo) * 100
                        carga_total_trabajo = round(carga_total_trabajo, 2)
                        cursor.execute(
                            f"""
                            UPDATE {tenant}.tb_carga_trabajo
                            SET carga_total_trabajo = %s
                            WHERE funcionario_id = %s
                            """,
                            (carga_total_trabajo, funcionario_id)
                        )
                        st.write(f"Carga total de trabajo actualizada: {carga_total_trabajo}%")
                
                # Insertar en el historial
                nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                empresa = st.session_state.get('empresa', 'Desconocida')
                accion = f"Agregó el funcionario {nombre} con ID {funcionario_id}"

                cursor.execute(f"""
                    INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                    VALUES (%s, %s, %s, %s)
                """, (nombre_usuario, nombre_usuario, empresa, accion))

                conexion.commit()
                st.success("Funcionario agregado correctamente.")
            except Exception as e:
                st.error(f"Error al agregar funcionario: {e}")
            finally:
                cursor.close()
                conexion.close()

# Función para agregar actividad
def agregar_actividad():
    
    # Asegúrate de obtener el esquema (tenant) de la sesión
    tenant = st.session_state.get('empresa', 'public')  # Obtenemos el esquema desde la sesión del usuario

    if not tenant:
        st.error("No se ha definido el esquema de la empresa.")
        return

    # Campos del formulario
    funcionario_id = st.text_input("ID del funcionario:")
    funcion = st.text_input("Función de la actividad:")
    cantidad = st.number_input("Cantidad de veces realizada:", min_value=1, step=1)
    tiempo_minimo = st.number_input("Tiempo mínimo:", min_value=0.0, step=0.1)
    tiempo_medio = st.number_input("Tiempo medio:", min_value=0.0, step=0.1)
    tiempo_maximo = st.number_input("Tiempo máximo:", min_value=0.0, step=0.1)
    unidad = st.selectbox("Unidad (minutos u horas):", ["minutos", "horas"])
    comentarios = st.text_area("Comentarios de la actividad:")

    if st.button("Agregar Actividad"):
        if not (funcionario_id and funcion and cantidad and tiempo_minimo and tiempo_medio and tiempo_maximo and unidad):
            st.warning("Todos los campos deben ser completados.")
            return

        if unidad == "minutos":
            tiempo_minimo /= 60
            tiempo_medio /= 60
            tiempo_maximo /= 60

        tiempo_por_actividad = (tiempo_minimo + 4 * tiempo_medio + tiempo_maximo) / 6 * cantidad

        # Conectar a la base de datos
        conexion = get_connection(tenant)  # Usamos el esquema dinámico aquí
        if conexion:
            cursor = conexion.cursor()
            try:
                # Verificar si el ID del funcionario existe
                cursor.execute(f'SELECT * FROM {tenant}.tb_funcionarios WHERE "Id" = %s', (funcionario_id,))
                if not cursor.fetchone():
                    st.warning(f"El ID {funcionario_id} no existe en la base de datos.")
                    return

                # Obtener el número de actividad más alto ya asignado para el funcionario
                cursor.execute(f'SELECT MAX(numero_actividad) FROM {tenant}.tb_actividades WHERE id_funcionario = %s', (funcionario_id,))
                max_numero_actividad = cursor.fetchone()[0]

                # Si no hay ninguna actividad asignada, comenzar con el número 1
                if max_numero_actividad is None:
                    numero_actividad = 1
                else:
                    numero_actividad = max_numero_actividad + 1

                # Insertar la actividad
                cursor.execute(
                    f"""
                    INSERT INTO {tenant}.tb_actividades 
                    (id_funcionario, numero_actividad, funcion, cantidad, tiempo_minimo, tiempo_medio, tiempo_maximo, unidad, comentarios)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (funcionario_id, numero_actividad, funcion, cantidad, tiempo_minimo, tiempo_medio, tiempo_maximo, unidad, comentarios)
                )

                # Actualizar el tiempo por actividad
                cursor.execute(
                    f"""
                    UPDATE {tenant}.tb_actividades
                    SET tiempo_por_actividad = %s
                    WHERE id_funcionario = %s AND numero_actividad = %s
                    """,
                    (tiempo_por_actividad, funcionario_id, numero_actividad)
                )

                # Actualizar el tiempo laborado en la tabla tb_carga_trabajo
                cursor.execute(
                    f"""
                    UPDATE {tenant}.tb_carga_trabajo
                    SET tiempo_laborado = (
                        SELECT SUM(tiempo_por_actividad)
                        FROM {tenant}.tb_actividades
                        WHERE id_funcionario = %s
                        GROUP BY id_funcionario
                    )
                    WHERE funcionario_id = %s;
                    """,
                    (funcionario_id, funcionario_id)
                )

                # Verificar si el funcionario ya tiene un registro en tb_carga_trabajo
                cursor.execute(f'SELECT * FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s', (funcionario_id,))
                if not cursor.fetchone():
                    cursor.execute(
                        f"""
                        INSERT INTO {tenant}.tb_carga_trabajo (funcionario_id, tiempo_laborado)
                        VALUES (%s, (
                            SELECT SUM(tiempo_por_actividad)
                            FROM {tenant}.tb_actividades
                            WHERE id_funcionario = %s
                            GROUP BY id_funcionario
                        ));
                        """,
                        (funcionario_id, funcionario_id)
                    )

                # Actualizar el campo carga_total_trabajo después de agregar la actividad
                cursor.execute(f"SELECT horas_trabajo, tiempo_laborado FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (funcionario_id,))
                row = cursor.fetchone()
                if row:
                    horas_trabajo = row[0] if row[0] else 1  # Evitar división por cero
                    tiempo_laborado = row[1] if row[1] else 0
                    carga_total_trabajo = (tiempo_laborado / horas_trabajo) * 100
                    carga_total_trabajo = round(carga_total_trabajo, 2)

                    # Actualizar el campo carga_total_trabajo en la base de datos
                    cursor.execute(
                        f"UPDATE {tenant}.tb_carga_trabajo SET carga_total_trabajo = %s WHERE funcionario_id = %s",
                        (carga_total_trabajo, funcionario_id)
                    )

                # Registrar la acción en el historial
                nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                empresa = st.session_state.get('empresa', 'Desconocida')
                accion = f"Agregó actividad {numero_actividad} para el funcionario con ID {funcionario_id}"

                cursor.execute(f"""
                    INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                    VALUES (%s, %s, %s, %s)
                """, (nombre_usuario, nombre_usuario, empresa, accion))
                conexion.commit()

                # Confirmar la transacción
                conexion.commit()
                st.success(f"Actividad {numero_actividad} agregada correctamente y tiempo laborado actualizado.")
            except Exception as e:
                st.error(f"Error al agregar la actividad: {e}")
            finally:
                cursor.close()
                conexion.close()


# Función para eliminar actividades seleccionadas y actualizar la carga total de trabajo
def eliminar_actividad():
    # Verificar que el tenant esté definido en st.session_state
    tenant = st.session_state.get('empresa', 'public')
    
    # Validar que el tenant sea uno de los esquemas permitidos o "public"
    if not tenant or tenant not in ['empresa1', 'empresa2', 'empresa3', 'empresa4', 'empresa5', 'public']:
        st.error("No se ha definido la empresa (tenant) correctamente. Por favor, seleccione una empresa válida.")
        return

    # Solicitar el ID del funcionario con un key único
    funcionario_id = st.text_input("ID del funcionario:", key="funcionario_id_input")

    if funcionario_id:
        # Conectar a la base de datos usando get_connection y pasando el tenant
        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                # Verificar si el ID del funcionario existe en el esquema correspondiente
                cursor.execute(f'SELECT * FROM {tenant}.tb_funcionarios WHERE "Id" = %s', (funcionario_id,))
                if not cursor.fetchone():
                    st.warning(f"El ID {funcionario_id} no existe en la base de datos.")
                    return

                # Obtener las actividades del funcionario
                cursor.execute(f''' 
                    SELECT numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, 
                           id, cantidad, tiempo_minimo, unidad, comentarios
                    FROM {tenant}.tb_actividades 
                    WHERE id_funcionario = %s
                ''', (funcionario_id,))
                actividades = cursor.fetchall()
                
                if not actividades:
                    st.warning(f"El funcionario con ID {funcionario_id} no tiene actividades registradas.")
                    return

                # Crear una lista de diccionarios con los datos de las actividades
                actividades_data = []
                for actividad in actividades:
                    numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, id_actividad, cantidad, tiempo_minimo, unidad, comentarios = actividad
                    actividad_dict = {
                        "Numero Actividad": numero_actividad,
                        "Funcion": funcion,
                        "Tiempo por Actividad (hrs)": tiempo_por_actividad,
                        "Tiempo Medio (hrs)": tiempo_medio,
                        "Tiempo Maximo (hrs)": tiempo_maximo,
                        "Cantidad": cantidad,
                        "Tiempo Minimo (hrs)": tiempo_minimo,
                        "Unidad": unidad,
                        "Comentarios": comentarios,
                    }
                    actividades_data.append(actividad_dict)

                # Convertir la lista de diccionarios en un DataFrame de pandas
                df_actividades = pd.DataFrame(actividades_data)

                # Mostrar la tabla con las actividades
                st.dataframe(df_actividades)

                # Selección de actividad para eliminar (usar el número de actividad)
                actividad_a_eliminar = st.selectbox("Seleccione la actividad a eliminar", [actividad[0] for actividad in actividades])

                # Confirmación de eliminación
                if st.button("Confirmar eliminación de actividad seleccionada"):
                    # Buscar el tiempo de la actividad seleccionada
                    cursor.execute(f''' 
                        SELECT tiempo_por_actividad FROM {tenant}.tb_actividades
                        WHERE id_funcionario = %s AND numero_actividad = %s
                    ''', (funcionario_id, actividad_a_eliminar))
                    actividad_data = cursor.fetchone()
                    if actividad_data:
                        tiempo_por_actividad = actividad_data[0]

                        # Eliminar la actividad seleccionada
                        cursor.execute(
                            f'DELETE FROM {tenant}.tb_actividades WHERE id_funcionario = %s AND numero_actividad = %s',
                            (funcionario_id, actividad_a_eliminar)
                        )

                        # Actualizar el tiempo laborado en la tabla tb_carga_trabajo
                        cursor.execute(
                            f'SELECT tiempo_laborado FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s', 
                            (funcionario_id,)
                        )
                        carga_trabajo_data = cursor.fetchone()
                        if carga_trabajo_data:
                            tiempo_laborado_actual = carga_trabajo_data[0]

                            # Restar el tiempo de la actividad eliminada
                            nuevo_tiempo_laborado = tiempo_laborado_actual - tiempo_por_actividad

                            # Actualizar carga laboral
                            cursor.execute(
                                f'''
                                UPDATE {tenant}.tb_carga_trabajo
                                SET tiempo_laborado = %s
                                WHERE funcionario_id = %s
                                ''',
                                (nuevo_tiempo_laborado, funcionario_id)
                            )

                            # Ahora actualizar el campo carga_total_trabajo
                            cursor.execute(f"SELECT horas_trabajo FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (funcionario_id,))
                            row = cursor.fetchone()
                            if row:
                                horas_trabajo = row[0] if row[0] else 1  # Evitar división por cero
                                carga_total_trabajo = (nuevo_tiempo_laborado / horas_trabajo) * 100
                                carga_total_trabajo = round(carga_total_trabajo, 2)

                                # Actualizar el campo carga_total_trabajo en la base de datos
                                cursor.execute(
                                    f"UPDATE {tenant}.tb_carga_trabajo SET carga_total_trabajo = %s WHERE funcionario_id = %s",
                                    (carga_total_trabajo, funcionario_id)
                                )

                        # Insertar en el historial de modificaciones
                        nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                        accion = f"Eliminó la actividad {actividad_a_eliminar} para el funcionario con ID {funcionario_id}"

                        cursor.execute(f"""
                            INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                            VALUES (%s, %s, %s, %s)
                        """, (nombre_usuario, nombre_usuario, tenant, accion))
                        conexion.commit()

                        # Confirmar la transacción
                        conexion.commit()
                        st.success(f"La actividad {actividad_a_eliminar} ha sido eliminada correctamente. Carga laboral actualizada.")
                    else:
                        st.warning(f"No se pudo encontrar el tiempo de la actividad {actividad_a_eliminar}.")
            except Exception as e:
                st.error(f"Error al eliminar la actividad: {e}")
            finally:
                cursor.close()
                conexion.close()


#Modificar actividad menu admin
def modificar_actividad():
    # Verificar si el tenant (empresa) está definido en la sesión
    tenant = st.session_state.get('empresa', 'public')
    if not tenant:
        st.error("No se ha seleccionado una empresa. Por favor, seleccione una empresa para continuar.")
        return

    # Solicitar el ID del funcionario con un key único
    funcionario_id = st.text_input("ID del funcionario:", key="funcionario_id_modificar")

    if funcionario_id:
        # Conectar a la base de datos usando get_connection
        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                # Verificar si el ID del funcionario existe en el esquema del tenant (empresa)
                cursor.execute(f'SELECT * FROM {tenant}.tb_funcionarios WHERE "Id" = %s', (funcionario_id,))
                if not cursor.fetchone():
                    st.warning(f"El ID {funcionario_id} no existe en la base de datos.")
                    return

                # Obtener las actividades del funcionario
                cursor.execute(f''' 
                    SELECT numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, 
                           id, cantidad, tiempo_minimo, unidad, comentarios
                    FROM {tenant}.tb_actividades 
                    WHERE id_funcionario = %s
                ''', (funcionario_id,))
                actividades = cursor.fetchall()

                if not actividades:
                    st.warning(f"El funcionario con ID {funcionario_id} no tiene actividades registradas.")
                    return

                # Crear una lista de diccionarios con los datos de las actividades
                actividades_data = []
                for actividad in actividades:
                    numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, id_actividad, cantidad, tiempo_minimo, unidad, comentarios = actividad
                    actividad_dict = {
                        "Numero Actividad": numero_actividad,
                        "Funcion": funcion,
                        "Tiempo por Actividad (hrs)": Decimal(str(tiempo_por_actividad)),  # Convertir a Decimal
                        "Tiempo Medio (hrs)": Decimal(str(tiempo_medio)),  # Convertir a Decimal
                        "Tiempo Maximo (hrs)": Decimal(str(tiempo_maximo)),  # Convertir a Decimal
                        "Cantidad": Decimal(str(cantidad)),  # Convertir a Decimal
                        "Tiempo Minimo (hrs)": Decimal(str(tiempo_minimo)),  # Convertir a Decimal
                        "Unidad": unidad,
                        "Comentarios": comentarios,
                    }
                    actividades_data.append(actividad_dict)

                # Convertir la lista de diccionarios en un DataFrame de pandas
                df_actividades = pd.DataFrame(actividades_data)

                # Mostrar la tabla con las actividades
                st.dataframe(df_actividades)

                # Selección de actividad para modificar (usar el número de actividad)
                actividad_a_modificar = st.selectbox("Seleccione la actividad a modificar", [actividad[0] for actividad in actividades])

                # Obtener los datos actuales de la actividad seleccionada
                cursor.execute(f''' 
                    SELECT funcion, tiempo_por_actividad, cantidad, tiempo_minimo, unidad, comentarios 
                    FROM {tenant}.tb_actividades 
                    WHERE id_funcionario = %s AND numero_actividad = %s
                ''', (funcionario_id, actividad_a_modificar))
                actividad_data = cursor.fetchone()

                if actividad_data:
                    funcion_actual, tiempo_por_actividad_actual, cantidad_actual, tiempo_minimo_actual, unidad_actual, comentarios_actual = actividad_data

                    # Crear los campos de entrada para editar los datos
                    funcion_nueva = st.text_input("Función", value=funcion_actual)
                    cantidad_nueva = st.number_input("Cantidad", value=int(cantidad_actual), min_value=0)
                    tiempo_minimo_nuevo = st.number_input("Tiempo mínimo (hrs)", value=float(tiempo_minimo_actual), min_value=0.0, step=0.1)
                    tiempo_medio_nuevo = st.number_input("Tiempo medio (hrs)", value=float(tiempo_por_actividad_actual), min_value=0.0, step=0.1)
                    tiempo_maximo_nuevo = st.number_input("Tiempo máximo (hrs)", value=float(tiempo_por_actividad_actual), min_value=0.0, step=0.1)
                    unidad_nueva = st.text_input("Unidad", value=unidad_actual)
                    comentarios_nuevos = st.text_area("Comentarios", value=comentarios_actual)

                    # Confirmación de modificación
                    if st.button("Confirmar modificación de actividad seleccionada"):
                        # Validación de datos
                        if not funcion_nueva or not unidad_nueva:
                            st.warning("Por favor, completa todos los campos obligatorios.")
                            return

                        # Conversión segura
                        tiempo_medio_nuevo = Decimal(str(tiempo_medio_nuevo))
                        tiempo_maximo_nuevo = Decimal(str(tiempo_maximo_nuevo))
                        cantidad_nueva = Decimal(str(cantidad_nueva))
                        tiempo_minimo_nuevo = Decimal(str(tiempo_minimo_nuevo))

                        # Fórmula para calcular el tiempo por actividad (igual a agregar_actividad)
                        tiempo_por_actividad_nuevo = (tiempo_minimo_nuevo + 4 * tiempo_medio_nuevo + tiempo_maximo_nuevo) / 6 * cantidad_nueva

                        # Actualizar la actividad en la base de datos
                        cursor.execute(f''' 
                            UPDATE {tenant}.tb_actividades
                            SET funcion = %s, tiempo_medio = %s, tiempo_maximo = %s,
                                cantidad = %s, tiempo_minimo = %s, unidad = %s, comentarios = %s
                            WHERE id_funcionario = %s AND numero_actividad = %s
                        ''', (funcion_nueva, tiempo_medio_nuevo, tiempo_maximo_nuevo,
                              cantidad_nueva, tiempo_minimo_nuevo, unidad_nueva, comentarios_nuevos,
                              funcionario_id, actividad_a_modificar))

                        # Actualizar el tiempo por actividad
                        cursor.execute(f"""
                            UPDATE {tenant}.tb_actividades
                            SET tiempo_por_actividad = %s
                            WHERE id_funcionario = %s AND numero_actividad = %s
                        """,
                            (tiempo_por_actividad_nuevo, funcionario_id, actividad_a_modificar)
                        )

                        # Actualizar el tiempo laborado en la tabla tb_carga_trabajo
                        cursor.execute(f"""
                            UPDATE {tenant}.tb_carga_trabajo
                            SET tiempo_laborado = (
                                SELECT SUM(tiempo_por_actividad)
                                FROM {tenant}.tb_actividades
                                WHERE id_funcionario = %s
                                GROUP BY id_funcionario
                            )
                            WHERE funcionario_id = %s;
                        """,
                            (funcionario_id, funcionario_id)
                        )

                        # Verificar si el funcionario ya tiene un registro en tb_carga_trabajo
                        cursor.execute(f'SELECT * FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s', (funcionario_id,))
                        if not cursor.fetchone():
                            cursor.execute(f"""
                                INSERT INTO {tenant}.tb_carga_trabajo (funcionario_id, tiempo_laborado)
                                VALUES (%s, (
                                    SELECT SUM(tiempo_por_actividad)
                                    FROM {tenant}.tb_actividades
                                    WHERE id_funcionario = %s
                                    GROUP BY id_funcionario
                                ));
                            """,
                                (funcionario_id, funcionario_id)
                            )

                        # Actualizar el campo carga_total_trabajo después de modificar la actividad
                        cursor.execute(f"SELECT horas_trabajo, tiempo_laborado FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (funcionario_id,))
                        row = cursor.fetchone()
                        if row:
                            horas_trabajo = row[0] if row[0] else 1  # Evitar división por cero
                            tiempo_laborado = row[1] if row[1] else 0
                            carga_total_trabajo = (tiempo_laborado / horas_trabajo) * 100
                            carga_total_trabajo = round(carga_total_trabajo, 2)

                            # Actualizar el campo carga_total_trabajo en la base de datos
                            cursor.execute(
                                f"UPDATE {tenant}.tb_carga_trabajo SET carga_total_trabajo = %s WHERE funcionario_id = %s",
                                (carga_total_trabajo, funcionario_id)
                            )

                        # Confirmar la transacción
                        conexion.commit()

                        # Insertar en el historial de modificaciones
                        nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                        empresa = st.session_state.get('empresa', 'Desconocida')
                        accion = f"Modificó la actividad {actividad_a_modificar} del funcionario con ID {funcionario_id}"

                        try:
                            cursor.execute(f"""
                                INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                                VALUES (%s, %s, %s, %s)
                            """, (nombre_usuario, nombre_usuario, empresa, accion))
                            conexion.commit()
                        except Exception as e:
                            st.error(f"Error al registrar la modificación en el historial: {e}")

                        st.success(f"La actividad {actividad_a_modificar} ha sido modificada correctamente. Carga laboral actualizada.")

            except Exception as e:
                st.error(f"Error al modificar la actividad: {e}")
            finally:
                cursor.close()
                conexion.close()


# Crear sección para administrar usuarios
def crear_cuenta():
    st.subheader("🧑‍💼 Gestión de Cuentas de Usuario")

    if "empresa" not in st.session_state:
        st.session_state["empresa"] = "public"  # Esquema por defecto en caso de que no haya selección

    esquema = st.session_state["empresa"]  # Usamos 'empresa' en lugar de 'esquema'

    # Verificación del esquema
    if esquema not in ["public", "empresa1", "empresa2", "empresa3", "empresa4", "empresa5"]:
        st.error(f"Esquema '{esquema}' no válido.")
        return

    # Conectar a la base de datos
    conn = get_connection(esquema)
    if not conn:
        st.error("Error al conectar a la base de datos.")
        return

    cursor = conn.cursor()  # Crear el cursor aquí

    # Ver usuarios existentes
    st.markdown("### 👥 Usuarios Existentes")
    try:
        cursor.execute(f"SELECT id, usuario, rol FROM {esquema}.tb_usuarios ORDER BY id")
        rows = cursor.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=["ID", "Usuario", "Rol"])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No hay usuarios registrados en este esquema.")
    except Exception as e:
        st.error(f"Error al consultar los usuarios: {e}")

    # Crear nueva cuenta
    st.markdown("### ➕ Crear Nueva Cuenta")
    nuevo_id = st.text_input("ID del usuario")
    nuevo_usuario = st.text_input("Nombre de usuario")
    nueva_contrasena = st.text_input("Contraseña", type="password")
    nuevo_rol = st.selectbox("Rol del usuario", ["Usuario", "Administrador"])

    if st.button("Crear cuenta"):
        if nuevo_id and nuevo_usuario and nueva_contrasena:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {esquema}.tb_usuarios WHERE id = %s", (nuevo_id,))
                if cursor.fetchone()[0] > 0:
                    st.warning("Ya existe un usuario con esa cédula (ID).")
                else:
                    cursor.execute(f"""
                        INSERT INTO {esquema}.tb_usuarios (id, usuario, contrasena, rol)
                        VALUES (%s, %s, %s, %s)
                    """, (nuevo_id, nuevo_usuario, nueva_contrasena, nuevo_rol))
                    conn.commit()

                    # Registrar la acción en el historial
                    nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                    empresa = st.session_state.get('empresa', 'Desconocida')
                    accion = f"Agregó el usuario {nuevo_usuario} con ID {nuevo_id}"

                    cursor.execute(f"""
                        INSERT INTO {esquema}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                        VALUES (%s, %s, %s, %s)
                    """, (nombre_usuario, nombre_usuario, empresa, accion))
                    conn.commit()

                    st.success(f"Usuario '{nuevo_usuario}' creado correctamente.")
                    st.rerun()  # Usamos st.rerun() para recargar la aplicación
            except Exception as e:
                st.error(f"Error al crear el usuario: {e}")
        else:
            st.warning("Por favor completa todos los campos.")

    # Modificar usuario existente
    st.markdown("### ✏️ Modificar Usuario Existente")
    id_mod = st.number_input("ID del usuario a modificar", min_value=1, step=1)
    nuevo_nombre = st.text_input("Nuevo nombre de usuario", key="mod_nombre")
    nueva_pass = st.text_input("Nueva contraseña", type="password", key="mod_pass")
    nuevo_rol_mod = st.selectbox("Nuevo rol", ["Usuario", "Administrador"], key="mod_rol")

    if st.button("Actualizar usuario"):
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {esquema}.tb_usuarios WHERE id = %s", (id_mod,))
            if cursor.fetchone()[0] == 0:
                st.warning("No existe un usuario con ese ID.")
            else:
                cursor.execute(f"""
                    UPDATE {esquema}.tb_usuarios
                    SET usuario = %s, contrasena = %s, rol = %s
                    WHERE id = %s
                """, (nuevo_nombre, nueva_pass, nuevo_rol_mod, id_mod))
                conn.commit()
                st.success("Usuario actualizado correctamente.")

                # Registrar la acción en el historial
                nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                empresa = st.session_state.get('empresa', 'Desconocida')
                accion = f"Actualizó el usuario con ID {id_mod}"

                cursor.execute(f"""
                    INSERT INTO {esquema}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                    VALUES (%s, %s, %s, %s)
                """, (nombre_usuario, nombre_usuario, empresa, accion))
                conn.commit()

                # Si el usuario actualizado es el mismo que el que está logueado (usualmente el admin),
                # actualizar el rol en la sesión para reflejar los cambios.
                if id_mod == st.session_state.usuario_id:  # Asegúrate de que "usuario_id" esté definido
                    st.session_state.rol = nuevo_rol_mod  # Actualiza el rol en la sesión del usuario.

                st.rerun()  # Recargar la aplicación
        except Exception as e:
            st.error(f"Error al actualizar el usuario: {e}")
    # Cerrar cursor y conexión
    cursor.close()
    conn.close()

    ##////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

import re

def reemplazar_accion(texto):
    texto = re.sub(r'Vió todas las cargas de trabajo de los funcionarios',
                   'Visualización de todas las cargas de trabajo', texto)

    texto = re.sub(r'Vió la carga de trabajo del funcionario con ID (\d+)',
                   r'Visualización de carga de trabajo del funcionario con ID \1', texto)

    texto = re.sub(r'Eliminó la actividad N° (\d+) del funcionario con ID (\d+)',
                   r'Eliminación de Actividad N° \1 del funcionario con ID \2', texto)

    texto = re.sub(r'Eliminó la actividad para el funcionario con ID (\d+)',
                   r'Eliminación de actividad para el funcionario con ID \1', texto)

    texto = re.sub(r'Subió actividades para el funcionario con ID (\d+)',
                   r'Importación de Actividades para el funcionario con ID \1', texto)
    
    texto = re.sub(r'Eliminó la actividad (\d+) para el funcionario con ID (\d+)',
               r'Eliminación de Actividad N° \1 del funcionario con ID \2', texto)


    return texto




#Historial de mosififcaciones

def mostrar_historial_modificaciones():
    esquemas_validos = ['empresa1', 'empresa2', 'empresa3', 'empresa4', 'empresa5', 'public']
    esquema = st.session_state.get('empresa', 'public')

    if esquema not in esquemas_validos:
        st.error("❌ Esquema no válido.")
        return

    try:
        if 'conn' not in st.session_state or st.session_state.conn is None:
            st.session_state.conn = get_connection(esquema)

        if st.session_state.conn:
            with st.session_state.conn.cursor() as cursor:
                consulta = f'''
                    SELECT usuario_id, nombre_usuario, empresa, accion, fecha
                    FROM {esquema}.tb_historial_modificaciones
                    ORDER BY fecha DESC
                '''
                cursor.execute(consulta)
                columnas = [desc[0] for desc in cursor.description]
                filas = cursor.fetchall()
                
                if filas:
                    # Crear DataFrame
                    df = pd.DataFrame(filas, columns=['ID Usuario', 'Nombre de Usuario', 'Empresa', 'Acción', 'Fecha'])
                    st.subheader(f"📄 Historial de Modificaciones ({esquema})")
                    df['Acción'] = df['Acción'].apply(reemplazar_accion)

                    # Añadir estilo con HTML
                    st.markdown("""
                        <style>
                            .stTable td, .stTable th {
                                padding: 8px;
                                text-align: left;
                            }
                            .stTable {
                                width: 100% !important;
                                table-layout: auto !important;
                            }
                        </style>
                    """, unsafe_allow_html=True)

                    # Mostrar la tabla con un estilo personalizado
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning(f"No hay modificaciones registradas en el esquema '{esquema}'.")
        else:
            st.error("❌ No se pudo establecer la conexión con la base de datos.")
    except Exception as e:
        st.error(f"Error al cargar el historial desde el esquema '{esquema}': {e}")



#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
from decimal import Decimal

# Función modificar actividad menú usuario
def modificar_actividadU():
    tenant = st.session_state.get('empresa', 'public')  # Verificación de empresa
    if not tenant:
        st.error("No se ha seleccionado una empresa. Por favor, selecciona una empresa.")
        return

    funcionario_id_login = st.session_state.usuario_id
    st.info(f"ID del funcionario logueado: {funcionario_id_login}")

    conexion = get_connection(tenant)
    if conexion:
        cursor = conexion.cursor()
        try:
            cursor.execute(f''' 
                SELECT numero_actividad, funcion, tiempo_medio, tiempo_maximo, 
                       id, cantidad, tiempo_minimo, unidad, comentarios
                FROM {tenant}.tb_actividades 
                WHERE id_funcionario = %s
            ''', (funcionario_id_login,))
            actividades = cursor.fetchall()

            if not actividades:
                st.warning("No tiene actividades registradas.")
                return

            actividades_data = []
            for actividad in actividades:
                numero_actividad, funcion, tiempo_medio, tiempo_maximo, id_actividad, cantidad, tiempo_minimo, unidad, comentarios = actividad
                actividad_dict = {
                    "Número Actividad": numero_actividad,
                    "Función": funcion,
                    "Cantidad": Decimal(str(cantidad)),
                    "Tiempo Mínimo (hrs)": Decimal(str(tiempo_minimo)),
                    "Tiempo Medio (hrs)": Decimal(str(tiempo_medio)),
                    "Tiempo Máximo (hrs)": Decimal(str(tiempo_maximo)),
                    "Unidad": unidad,
                    "Comentarios": comentarios,
                }
                actividades_data.append(actividad_dict)

            df_actividades = pd.DataFrame(actividades_data)
            st.dataframe(df_actividades)

            actividad_a_modificar = st.selectbox("Seleccione la actividad a modificar", [actividad[0] for actividad in actividades])

            cursor.execute(f''' 
                SELECT funcion, tiempo_medio, tiempo_maximo, cantidad, tiempo_minimo, unidad, comentarios 
                FROM {tenant}.tb_actividades 
                WHERE id_funcionario = %s AND numero_actividad = %s
            ''', (funcionario_id_login, actividad_a_modificar))
            actividad_data = cursor.fetchone()

            if actividad_data:
                funcion_actual, tiempo_medio_actual, tiempo_maximo_actual, cantidad_actual, tiempo_minimo_actual, unidad_actual, comentarios_actual = actividad_data

                # Mostrar los campos en el orden solicitado
                funcion_nueva = st.text_input("Función", value=funcion_actual)
                cantidad_nueva = st.number_input("Cantidad", value=int(cantidad_actual), min_value=0)
                tiempo_minimo_nuevo = st.number_input("Tiempo mínimo (hrs)", value=float(tiempo_minimo_actual), min_value=0.0, step=0.1)
                tiempo_medio_nuevo = st.number_input("Tiempo medio (hrs)", value=float(tiempo_medio_actual), min_value=0.0, step=0.1)
                tiempo_maximo_nuevo = st.number_input("Tiempo máximo (hrs)", value=float(tiempo_maximo_actual), min_value=0.0, step=0.1)
                unidad_nueva = st.text_input("Unidad", value=unidad_actual)
                comentarios_nuevos = st.text_area("Comentarios", value=comentarios_actual)

                if st.button("Confirmar modificación de actividad seleccionada"):
                    if not funcion_nueva or not unidad_nueva:
                        st.warning("Por favor, completa todos los campos obligatorios.")
                        return

                    # Conversión segura
                    tiempo_medio_nuevo = Decimal(str(tiempo_medio_nuevo))
                    tiempo_maximo_nuevo = Decimal(str(tiempo_maximo_nuevo))
                    cantidad_nueva = Decimal(str(cantidad_nueva))
                    tiempo_minimo_nuevo = Decimal(str(tiempo_minimo_nuevo))

                    # Fórmula para calcular el tiempo laborado por actividad
                    tiempo_laborado_nuevo = (tiempo_minimo_nuevo + 4 * tiempo_medio_nuevo + tiempo_maximo_nuevo) / 6 * cantidad_nueva

                    cursor.execute(f''' 
                        UPDATE {tenant}.tb_actividades
                        SET funcion = %s, tiempo_medio = %s, tiempo_maximo = %s,
                            cantidad = %s, tiempo_minimo = %s, unidad = %s, comentarios = %s
                        WHERE id_funcionario = %s AND numero_actividad = %s
                    ''', (funcion_nueva, tiempo_medio_nuevo, tiempo_maximo_nuevo,
                          cantidad_nueva, tiempo_minimo_nuevo, unidad_nueva, comentarios_nuevos,
                          funcionario_id_login, actividad_a_modificar))

                    # Actualizar el tiempo por actividad
                    cursor.execute(f'''
                        UPDATE {tenant}.tb_actividades
                        SET tiempo_por_actividad = %s
                        WHERE id_funcionario = %s AND numero_actividad = %s
                    ''',
                        (tiempo_laborado_nuevo, funcionario_id_login, actividad_a_modificar)
                    )

                    # Actualizar el tiempo laborado en la tabla tb_carga_trabajo
                    cursor.execute(f'''
                        UPDATE {tenant}.tb_carga_trabajo
                        SET tiempo_laborado = (
                            SELECT SUM(tiempo_por_actividad)
                            FROM {tenant}.tb_actividades
                            WHERE id_funcionario = %s
                            GROUP BY id_funcionario
                        )
                        WHERE funcionario_id = %s;
                    ''', (funcionario_id_login, funcionario_id_login))

                    # Verificar si el funcionario ya tiene un registro en tb_carga_trabajo
                    cursor.execute(f'SELECT * FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s', (funcionario_id_login,))
                    if not cursor.fetchone():
                        cursor.execute(f'''
                            INSERT INTO {tenant}.tb_carga_trabajo (funcionario_id, tiempo_laborado)
                            VALUES (%s, (
                                SELECT SUM(tiempo_por_actividad)
                                FROM {tenant}.tb_actividades
                                WHERE id_funcionario = %s
                                GROUP BY id_funcionario
                            ));
                        ''', (funcionario_id_login, funcionario_id_login))

                    # Actualizar el campo carga_total_trabajo
                    cursor.execute(f"SELECT horas_trabajo, tiempo_laborado FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (funcionario_id_login,))
                    row = cursor.fetchone()
                    if row:
                        horas_trabajo = row[0] if row[0] else 1  # Evitar división por cero
                        tiempo_laborado = row[1] if row[1] else 0
                        carga_total_trabajo = (tiempo_laborado / horas_trabajo) * 100
                        carga_total_trabajo = round(carga_total_trabajo, 2)

                        cursor.execute(f'''
                            UPDATE {tenant}.tb_carga_trabajo SET carga_total_trabajo = %s WHERE funcionario_id = %s
                        ''', (carga_total_trabajo, funcionario_id_login))

                    # Insertar en el historial de modificaciones
                    nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
                    empresa = st.session_state.get('empresa', 'Desconocida')
                    accion = f"Modificó la actividad N° {actividad_a_modificar} del funcionario con ID {funcionario_id_login}"

                    cursor.execute(f"""
                        INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                        VALUES (%s, %s, %s, %s)
                    """, (funcionario_id_login, nombre_usuario, empresa, accion))

                    conexion.commit()
                    st.success(f"La actividad {actividad_a_modificar} ha sido modificada correctamente. Carga laboral actualizada.")

        except Exception as e:
            st.error(f"Error al modificar la actividad: {e}")
        finally:
            cursor.close()
            conexion.close()

# Función eliminar actividad menú usuario con agregar historial
def eliminar_actividadU():
    tenant = st.session_state.get('empresa', 'public')  # Verificar si el tenant (empresa) está definido
    if not tenant:
        st.error("No se ha definido la empresa. Por favor, selecciona una empresa.")
        return

    funcionario_id = st.session_state.usuario_id
    nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
    empresa = st.session_state.get('empresa', 'Desconocida')
    conexion = get_connection(tenant)
    
    if conexion:
        cursor = conexion.cursor()
        try:
            # Obtener las actividades del funcionario autenticado
            cursor.execute(f''' 
                SELECT numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, 
                       id, cantidad, tiempo_minimo, unidad, comentarios
                FROM {tenant}.tb_actividades 
                WHERE id_funcionario = %s
            ''', (funcionario_id,))
            actividades = cursor.fetchall()

            if not actividades:
                st.warning("No tienes actividades registradas.")
                return

            actividades_data = []
            for actividad in actividades:
                numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, id_actividad, cantidad, tiempo_minimo, unidad, comentarios = actividad
                actividades_data.append({
                    "Numero Actividad": numero_actividad,
                    "Funcion": funcion,
                    "Tiempo por Actividad (hrs)": tiempo_por_actividad,
                    "Tiempo Medio (hrs)": tiempo_medio,
                    "Tiempo Máximo (hrs)": tiempo_maximo,
                    "Cantidad": cantidad,
                    "Tiempo Mínimo (hrs)": tiempo_minimo,
                    "Unidad": unidad,
                    "Comentarios": comentarios,
                })

            df_actividades = pd.DataFrame(actividades_data)
            st.dataframe(df_actividades)

            actividad_a_eliminar = st.selectbox("Seleccione la actividad a eliminar", [actividad[0] for actividad in actividades])

            if st.button("Confirmar eliminación de actividad seleccionada"):
                # Buscar todos los datos de la actividad para el historial
                cursor.execute(f''' 
                    SELECT numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, tiempo_maximo, 
                           cantidad, tiempo_minimo, unidad, comentarios
                    FROM {tenant}.tb_actividades
                    WHERE id_funcionario = %s AND numero_actividad = %s
                ''', (funcionario_id, actividad_a_eliminar))
                actividad_data = cursor.fetchone()

                if actividad_data:
                    (numero_actividad, funcion, tiempo_por_actividad, tiempo_medio, 
                     tiempo_maximo, cantidad, tiempo_minimo, unidad, comentarios) = actividad_data

                    # Insertar en el historial de modificaciones
                    accion = f"Eliminó la actividad N° {actividad_a_eliminar} del funcionario con ID {funcionario_id}"
                    cursor.execute(f"""
                        INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion, fecha)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """, (funcionario_id, nombre_usuario, empresa, accion))

                    # Insertar en el historial de actividad eliminada
                    cursor.execute(f'''
                        INSERT INTO {tenant}.tb_historial_modificaciones (
                            usuario_id, nombre_usuario, empresa, accion, fecha
                        ) VALUES (%s, %s, %s, 'Actividad eliminada', CURRENT_TIMESTAMP)
                    ''', (funcionario_id, nombre_usuario, empresa))

                    # Eliminar la actividad
                    cursor.execute(f'''
                        DELETE FROM {tenant}.tb_actividades WHERE id_funcionario = %s AND numero_actividad = %s
                    ''', (funcionario_id, actividad_a_eliminar))

                    # Actualizar tiempo laborado
                    cursor.execute(f'''
                        SELECT tiempo_laborado, horas_trabajo FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s
                    ''', (funcionario_id,))
                    carga_trabajo_data = cursor.fetchone()
                    if carga_trabajo_data:
                        tiempo_laborado_actual, horas_trabajo = carga_trabajo_data
                        nuevo_tiempo_laborado = tiempo_laborado_actual - tiempo_por_actividad
                        horas_trabajo = horas_trabajo if horas_trabajo else 1

                        carga_total_trabajo = (nuevo_tiempo_laborado / horas_trabajo) * 100
                        carga_total_trabajo = round(carga_total_trabajo, 2)

                        cursor.execute(f'''
                            UPDATE {tenant}.tb_carga_trabajo
                            SET tiempo_laborado = %s, carga_total_trabajo = %s
                            WHERE funcionario_id = %s
                        ''', (nuevo_tiempo_laborado, carga_total_trabajo, funcionario_id))

                    # Confirmar y cerrar
                    conexion.commit()
                    st.success(f"La actividad {actividad_a_eliminar} ha sido eliminada correctamente. Se ha registrado en el historial. Carga laboral actualizada.")

                    # Verificar si quedan actividades
                    cursor.execute(f'''
                        SELECT COUNT(*) FROM {tenant}.tb_actividades WHERE id_funcionario = %s
                    ''', (funcionario_id,))
                    actividades_restantes = cursor.fetchone()[0]
                    if actividades_restantes == 0:
                        st.info("Ya no quedan actividades registradas para este funcionario.")
                else:
                    st.warning(f"No se pudo encontrar la actividad {actividad_a_eliminar}.")
        except Exception as e:
            st.error(f"Error al eliminar la actividad: {e}")
        finally:
            cursor.close()
            conexion.close()


#Agregar actividad Usuario
def agregar_actividadU(funcionario_id):
    tenant = st.session_state.get('empresa', 'public')
    nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')

    st.header("Agregar Nueva Actividad")

    funcion = st.text_input("Función de la actividad:")
    cantidad = st.number_input("Cantidad de veces realizada:", min_value=1, step=1)
    tiempo_minimo = st.number_input("Tiempo mínimo:", min_value=0.0, step=0.1)
    tiempo_medio = st.number_input("Tiempo medio:", min_value=0.0, step=0.1)
    tiempo_maximo = st.number_input("Tiempo máximo:", min_value=0.0, step=0.1)
    unidad = st.selectbox("Unidad (minutos u horas):", ["minutos", "horas"])
    comentarios = st.text_area("Comentarios de la actividad:")

    if st.button("Agregar Actividad"):
        if not (funcion and cantidad and tiempo_minimo and tiempo_medio and tiempo_maximo and unidad):
            st.warning("Todos los campos deben ser completados.")
            return

        if unidad == "minutos":
            tiempo_minimo /= 60
            tiempo_medio /= 60
            tiempo_maximo /= 60

        tiempo_por_actividad = (tiempo_minimo + 4 * tiempo_medio + tiempo_maximo) / 6 * cantidad

        conexion = get_connection(tenant)
        if conexion:
            cursor = conexion.cursor()
            try:
                cursor.execute(
                    f'SELECT MAX(numero_actividad) FROM {tenant}.tb_actividades WHERE id_funcionario = %s',
                    (funcionario_id,)
                )
                max_numero_actividad = cursor.fetchone()[0]
                numero_actividad = 1 if max_numero_actividad is None else max_numero_actividad + 1

                cursor.execute(
                    f'''
                    INSERT INTO {tenant}.tb_actividades 
                    (id_funcionario, numero_actividad, funcion, cantidad, tiempo_minimo, 
                    tiempo_medio, tiempo_maximo, unidad, comentarios, tiempo_por_actividad)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''',
                    (funcionario_id, numero_actividad, funcion, cantidad, tiempo_minimo, 
                     tiempo_medio, tiempo_maximo, unidad, comentarios, tiempo_por_actividad)
                )

                cursor.execute(f'SELECT * FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s', (funcionario_id,))
                if not cursor.fetchone():
                    cursor.execute(
                        f'''
                        INSERT INTO {tenant}.tb_carga_trabajo (funcionario_id, tiempo_laborado)
                        VALUES (%s, %s)
                        ''',
                        (funcionario_id, tiempo_por_actividad)
                    )
                else:
                    cursor.execute(
                        f'''
                        UPDATE {tenant}.tb_carga_trabajo
                        SET tiempo_laborado = (
                            SELECT SUM(tiempo_por_actividad)
                            FROM {tenant}.tb_actividades
                            WHERE id_funcionario = %s
                        )
                        WHERE funcionario_id = %s
                        ''',
                        (funcionario_id, funcionario_id)
                    )

                cursor.execute(
                    f'''
                    SELECT horas_trabajo, tiempo_laborado
                    FROM {tenant}.tb_carga_trabajo
                    WHERE funcionario_id = %s
                    ''',
                    (funcionario_id,)
                )
                row = cursor.fetchone()
                if row:
                    horas_trabajo = row[0] if row[0] else 1
                    tiempo_laborado = row[1] if row[1] else 0
                    carga_total_trabajo = round((tiempo_laborado / horas_trabajo) * 100, 2)

                    cursor.execute(
                        f'''
                        UPDATE {tenant}.tb_carga_trabajo
                        SET carga_total_trabajo = %s
                        WHERE funcionario_id = %s
                        ''',
                        (carga_total_trabajo, funcionario_id)
                    )

                # INSERTAR EN EL HISTORIAL
                accion = f"Agregó actividad {numero_actividad} al funcionario ID {funcionario_id}"
                cursor.execute(
                    f'''
                    INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                    VALUES (%s, %s, %s, %s)
                    ''',
                    (nombre_usuario, nombre_usuario, tenant, accion)
                )

                conexion.commit()
                st.success(f"Actividad {numero_actividad} agregada correctamente. Tiempo laborado actualizado.")

            except Exception as e:
                st.error(f"Error al agregar la actividad: {e}")
            finally:
                cursor.close()
                conexion.close()



# Función para agregar un nuevo funcionario vinculado al usuario logueado
def agregar_funcionarioU():
    tenant = st.session_state.get('empresa', 'public')
    if not tenant:
        st.error("No se ha seleccionado una empresa. Por favor, seleccione una empresa antes de proceder.")
        return

    usuario_id_logueado = st.session_state.usuario_id

    conexion = get_connection(tenant)
    cursor = conexion.cursor()
    try:
        # Obtener el nombre del usuario logueado
        cursor.execute(f"SELECT usuario FROM {tenant}.tb_usuarios WHERE id = %s", (usuario_id_logueado,))
        row = cursor.fetchone()
        if row:
            nombre = row[0]
        else:
            st.error("No se encontró el usuario en la base de datos.")
            return
    except Exception as e:
        st.error(f"Error al obtener el nombre del usuario: {e}")
        return
    finally:
        cursor.close()
        conexion.close()

    dependencia = st.text_input("Dependencia")
    puesto = st.text_input("Puesto")
    jornada = st.number_input("Jornada (Horas/día)", min_value=0.0)
    feriados = st.number_input("Feriados (Días en el período de estudio)", min_value=0.0)
    horas_extra = st.number_input("Horas extra (Total de horas extra en el período de estudio)", min_value=0.0)
    vacaciones = st.number_input("Vacaciones (Total de días disfrutados en el período de estudio)", min_value=0.0)
    incapacidades = st.number_input("Incapacidades (Días de incapacidad sin considerar días no hábiles)", min_value=0.0)
    permiso = st.number_input("Permiso (Días hábiles solicitados)", min_value=0.0)
    comentarios = st.text_area("Comentarios")

    if st.button("Agregar Funcionario"):
        if not dependencia or not puesto:
            st.warning("Los campos Dependencia y Puesto son obligatorios.")
            return

        dias_laborales = 260
        total_laborable_base = jornada * dias_laborales
        horas_no_laborables = (vacaciones + feriados + incapacidades + permiso) * jornada
        total_laborable = total_laborable_base + horas_extra - horas_no_laborables

        st.write(f"Total Laborable Base: {total_laborable_base}")
        st.write(f"Horas No Laborables: {horas_no_laborables}")
        st.write(f"Total Laborable final: {total_laborable}")

        conexion = get_connection(tenant)
        cursor = conexion.cursor()
        try:
            # Verificar si el usuario ya tiene un funcionario registrado
            cursor.execute(f'SELECT "Id" FROM {tenant}.tb_funcionarios WHERE "Id" = %s', (usuario_id_logueado,))
            if cursor.fetchone():
                st.warning("Este usuario ya tiene un funcionario registrado.")
                return

            # Insertar nuevo funcionario
            cursor.execute(
                f"""INSERT INTO {tenant}.tb_funcionarios 
                ("Id", "Nombre", "Dependencia", "Puesto", "Jornada", "Feriados", "Horas_extra", 
                "Vacaciones", "Incapacidades", "Permiso", "Otro/Comentarios")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (usuario_id_logueado, nombre, dependencia, puesto, jornada, feriados, horas_extra,
                 vacaciones, incapacidades, permiso, comentarios)
            )

            id_funcionario = usuario_id_logueado

            # Insertar o actualizar carga de trabajo
            cursor.execute(f"SELECT * FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s", (id_funcionario,))
            if cursor.fetchone():
                cursor.execute(
                    f"""
                    UPDATE {tenant}.tb_carga_trabajo 
                    SET horas_trabajo = %s
                    WHERE funcionario_id = %s
                    """,
                    (total_laborable, id_funcionario)
                )
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {tenant}.tb_carga_trabajo (funcionario_id, horas_trabajo)
                    VALUES (%s, %s)
                    """,
                    (id_funcionario, total_laborable)
                )

            # Calcular y actualizar el porcentaje de carga total de trabajo
            cursor.execute(
                f"""
                SELECT tiempo_laborado, horas_trabajo 
                FROM {tenant}.tb_carga_trabajo 
                WHERE funcionario_id = %s
                """, (id_funcionario,)
            )
            row = cursor.fetchone()
            if row:
                tiempo_laborado = row[0] if row[0] else 0
                horas_trabajo = row[1] if row[1] else 1
                if horas_trabajo > 0:
                    carga_total_trabajo = round((tiempo_laborado / horas_trabajo) * 100, 2)
                    cursor.execute(
                        f"""
                        UPDATE {tenant}.tb_carga_trabajo
                        SET carga_total_trabajo = %s
                        WHERE funcionario_id = %s
                        """,
                        (carga_total_trabajo, id_funcionario)
                    )
                    st.write(f"Carga total de trabajo actualizada: {carga_total_trabajo}%")

            # Insertar en el historial de modificaciones
            nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
            empresa = st.session_state.get('empresa', 'Desconocida')
            accion = f"Agregó el funcionario {nombre} con ID {id_funcionario}"

            cursor.execute(f"""
                INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                VALUES (%s, %s, %s, %s)
            """, (usuario_id_logueado, nombre_usuario, empresa, accion))

            conexion.commit()
            st.success("Funcionario agregado correctamente.")
        except Exception as e:
            st.error(f"Error al agregar funcionario: {e}")
        finally:
            cursor.close()
            conexion.close()


#Subir actividades mediante excel
from decimal import Decimal

def cargar_actividades_excel(funcionario_id):
    st.header("Cargar Actividades desde Excel")

    archivo_excel = st.file_uploader("Sube un archivo Excel con las actividades", type=["xlsx"])

    if archivo_excel:
        try:
            df = pd.read_excel(archivo_excel)

            columnas_requeridas = ['funcion', 'cantidad', 'tiempo_minimo', 'tiempo_medio', 'tiempo_maximo', 'unidad', 'comentarios']
            if not all(col in df.columns for col in columnas_requeridas):
                st.error("El archivo debe contener las siguientes columnas: " + ", ".join(columnas_requeridas))
                return

            tenant = st.session_state.get("empresa")
            if not tenant:
                st.error("No se ha seleccionado una empresa.")
                return

            conn = get_connection(tenant)
            cursor = conn.cursor()

            # Insertar actividades
            for _, row in df.iterrows():
                # Cálculo de tiempo por actividad
                if row['unidad'] == 'minutos':
                    row['tiempo_minimo'] /= 60
                    row['tiempo_medio'] /= 60
                    row['tiempo_maximo'] /= 60

                # Convertir a Decimal antes de la operación
                cantidad = Decimal(row['cantidad']) if pd.notna(row['cantidad']) else Decimal(0)
                tiempo_minimo = Decimal(row['tiempo_minimo']) if pd.notna(row['tiempo_minimo']) else Decimal(0)
                tiempo_medio = Decimal(row['tiempo_medio']) if pd.notna(row['tiempo_medio']) else Decimal(0)
                tiempo_maximo = Decimal(row['tiempo_maximo']) if pd.notna(row['tiempo_maximo']) else Decimal(0)

                tiempo_por_actividad = (tiempo_minimo + (4 * tiempo_medio) + tiempo_maximo) / 6 * cantidad

                # Obtener el número de actividad más alto ya asignado
                cursor.execute(f"""
                    SELECT COALESCE(MAX(numero_actividad), 0) + 1 
                    FROM {tenant}.tb_actividades 
                    WHERE id_funcionario = %s
                """, (funcionario_id,))
                numero_actividad = cursor.fetchone()[0]

                cursor.execute(f"""
                    INSERT INTO {tenant}.tb_actividades 
                    (id_funcionario, numero_actividad, funcion, cantidad, tiempo_minimo, tiempo_medio, tiempo_maximo, unidad, comentarios, tiempo_por_actividad)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    funcionario_id,
                    numero_actividad,
                    row['funcion'],
                    cantidad,
                    tiempo_minimo,
                    tiempo_medio,
                    tiempo_maximo,
                    row['unidad'],
                    row['comentarios'],
                    tiempo_por_actividad
                ))

                # Actualizar tiempo laborado y carga total de trabajo
                cursor.execute(f'''
                    SELECT tiempo_laborado, horas_trabajo FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s
                ''', (funcionario_id,))
                carga_trabajo_data = cursor.fetchone()
                if carga_trabajo_data:
                    tiempo_laborado_actual, horas_trabajo = carga_trabajo_data
                    nuevo_tiempo_laborado = tiempo_laborado_actual + tiempo_por_actividad
                    horas_trabajo = horas_trabajo if horas_trabajo else 1  # Prevenir división por cero

                    carga_total_trabajo = (nuevo_tiempo_laborado / horas_trabajo) * 100
                    carga_total_trabajo = round(carga_total_trabajo, 2)

                    cursor.execute(f'''
                        UPDATE {tenant}.tb_carga_trabajo
                        SET tiempo_laborado = %s, carga_total_trabajo = %s
                        WHERE funcionario_id = %s
                    ''', (nuevo_tiempo_laborado, carga_total_trabajo, funcionario_id))

            # Registrar la acción en el historial
            nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
            empresa = st.session_state.get('empresa', 'Desconocida')
            accion = f"Subió actividades para el funcionario con ID {funcionario_id}"

            cursor.execute(f"""
                INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                VALUES (%s, %s, %s, %s)
            """, (funcionario_id, nombre_usuario, empresa, accion))

            conn.commit()
            st.success("Actividades cargadas y registradas en el historial exitosamente.")

        except Exception as e:
            st.error(f"Error al procesar el archivo: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()


#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


def cargar_actividades_excell():
    st.header("Cargar Actividades Excel")

    # Ingreso del ID del funcionario
    funcionario_id = st.text_input("Ingrese el ID del funcionario")

    if not funcionario_id:
        st.warning("Por favor, ingrese un ID de funcionario.")
        return

    archivo_excel = st.file_uploader("Sube un archivo Excel con las actividades", type=["xlsx"])

    if archivo_excel:
        try:
            df = pd.read_excel(archivo_excel)

            columnas_requeridas = ['funcion', 'cantidad', 'tiempo_minimo', 'tiempo_medio', 'tiempo_maximo', 'unidad', 'comentarios']
            if not all(col in df.columns for col in columnas_requeridas):
                st.error("El archivo debe contener las siguientes columnas: " + ", ".join(columnas_requeridas))
                return

            tenant = st.session_state.get("empresa")
            if not tenant:
                st.error("No se ha seleccionado una empresa.")
                return

            conn = get_connection(tenant)
            cursor = conn.cursor()

            # Insertar actividades
            for _, row in df.iterrows():
                # Cálculo de tiempo por actividad
                if row['unidad'] == 'minutos':
                    row['tiempo_minimo'] /= 60
                    row['tiempo_medio'] /= 60
                    row['tiempo_maximo'] /= 60

                cantidad = Decimal(row['cantidad']) if pd.notna(row['cantidad']) else Decimal(0)
                tiempo_minimo = Decimal(row['tiempo_minimo']) if pd.notna(row['tiempo_minimo']) else Decimal(0)
                tiempo_medio = Decimal(row['tiempo_medio']) if pd.notna(row['tiempo_medio']) else Decimal(0)
                tiempo_maximo = Decimal(row['tiempo_maximo']) if pd.notna(row['tiempo_maximo']) else Decimal(0)

                tiempo_por_actividad = (tiempo_minimo + (4 * tiempo_medio) + tiempo_maximo) / 6 * cantidad

                cursor.execute(f"""
                    SELECT COALESCE(MAX(numero_actividad), 0) + 1 
                    FROM {tenant}.tb_actividades 
                    WHERE id_funcionario = %s
                """, (funcionario_id,))
                numero_actividad = cursor.fetchone()[0]

                cursor.execute(f"""
                    INSERT INTO {tenant}.tb_actividades 
                    (id_funcionario, numero_actividad, funcion, cantidad, tiempo_minimo, tiempo_medio, tiempo_maximo, unidad, comentarios, tiempo_por_actividad)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    funcionario_id,
                    numero_actividad,
                    row['funcion'],
                    cantidad,
                    tiempo_minimo,
                    tiempo_medio,
                    tiempo_maximo,
                    row['unidad'],
                    row['comentarios'],
                    tiempo_por_actividad
                ))

                # Actualizar carga
                cursor.execute(f'''
                    SELECT tiempo_laborado, horas_trabajo FROM {tenant}.tb_carga_trabajo WHERE funcionario_id = %s
                ''', (funcionario_id,))
                carga_trabajo_data = cursor.fetchone()
                if carga_trabajo_data:
                    tiempo_laborado_actual, horas_trabajo = carga_trabajo_data
                    nuevo_tiempo_laborado = tiempo_laborado_actual + tiempo_por_actividad
                    horas_trabajo = horas_trabajo if horas_trabajo else 1

                    carga_total_trabajo = (nuevo_tiempo_laborado / horas_trabajo) * 100
                    carga_total_trabajo = round(carga_total_trabajo, 2)

                    cursor.execute(f'''
                        UPDATE {tenant}.tb_carga_trabajo
                        SET tiempo_laborado = %s, carga_total_trabajo = %s
                        WHERE funcionario_id = %s
                    ''', (nuevo_tiempo_laborado, carga_total_trabajo, funcionario_id))

            # Registrar en historial
            nombre_usuario = st.session_state.get('nombre_usuario', 'Desconocido')
            empresa = st.session_state.get('empresa', 'Desconocida')
            accion = f"Superadmin subió actividades para el funcionario con ID {funcionario_id}"

            cursor.execute(f"""
                INSERT INTO {tenant}.tb_historial_modificaciones (usuario_id, nombre_usuario, empresa, accion)
                VALUES (%s, %s, %s, %s)
            """, (funcionario_id, nombre_usuario, empresa, accion))

            conn.commit()
            st.success("Actividades cargadas exitosamente y registradas en el historial.")

        except Exception as e:
            st.error(f"Error al procesar el archivo: {e}")
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()


#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# Login
def login():
    st.title("🔐 Inicio de Sesión")
    
    # Selección de empresa
    empresa = st.selectbox("Selecciona la empresa a la que perteneces", [
        "empresa1", "empresa2", "empresa3", "empresa4", "empresa5", "public"
    ])
    
    # Ingreso de credenciales
    id_usuario = st.text_input("ID de Usuario")
    contrasena = st.text_input("Contraseña", type="password")

    if st.button("Iniciar Sesión"):
        if id_usuario and contrasena:
            try:
                # Obtener la conexión y verificar el usuario en el esquema de la empresa seleccionada
                conn = get_connection(empresa)  # Usamos la empresa seleccionada en la conexión
                cursor = conn.cursor()
                cursor.execute(""" 
                    SELECT usuario, rol FROM tb_usuarios 
                    WHERE id = %s AND contrasena = %s
                """, (id_usuario, contrasena))  # Usamos las columnas correctas
                resultado = cursor.fetchone()
                conn.close()

                if resultado:
                    nombre_usuario, rol = resultado
                    # Guardamos los datos en la sesión
                    st.session_state.logueado = True
                    st.session_state.rol = rol
                    st.session_state.usuario_id = id_usuario  # Guardamos el ID de usuario
                    st.session_state.nombre_usuario = nombre_usuario
                    st.session_state.empresa = empresa  # Guardamos la empresa seleccionada

                    st.success(f"Bienvenido {nombre_usuario} ({rol})")
                    st.rerun()  # Recargar la aplicación después de loguearse
                else:
                    st.error("ID o contraseña incorrectos.")
            except Exception as e:
                st.error(f"Error al conectar a la base de datos: {e}")
        else:
            st.warning("Por favor, completa todos los campos.")

# Estado de sesión
if "logueado" not in st.session_state:
    st.session_state.logueado = False
if "rol" not in st.session_state:
    st.session_state.rol = None
if "usuario_id" not in st.session_state:
    st.session_state.usuario_id = None
if "nombre_usuario" not in st.session_state:
    st.session_state.nombre_usuario = ""

# Home
def home():
    st.title("Bienvenido a la Aplicación de Workload")
    st.write(""" 
    Workload ofrece un seguimiento eficiente y en tiempo real de las asignaciones laborales, facilitando la gestión de tareas y aumentando la productividad en distintos departamentos o equipos. Cuenta con una interfaz intuitiva que permite monitorear cargas de trabajo, asignar responsabilidades y optimizar la distribución de tareas, garantizando así una operación más organizada y eficaz.
    """)

def menu_principal():
    if not st.session_state.logueado:
        login()
    else:
        st.sidebar.title("Menú Principal")
        st.sidebar.markdown(f"👤 **Usuario:** {st.session_state.nombre_usuario}")
        st.sidebar.markdown(f"🔑 **Rol:** {st.session_state.rol}")

        if st.sidebar.button("🔁 Cerrar sesión"):
            for key in ["logueado", "rol", "usuario_id", "nombre_usuario", "empresa"]:
                st.session_state.pop(key, None)
            st.rerun()

        # ----- SUPERADMIN -----
        if st.session_state.rol == "Superadmin":
            opcion = st.sidebar.radio("Selecciona una opción (Superadmin):", [
                "🏠 Inicio",
                "🔍 Ver Carga de Trabajo por ID",
                "📋 Ver Todas las Cargas de Trabajo",
                "➕ Detalles de Periodo de Estudio",
                "📝 Agregar Actividad",
                "❌ Eliminar Funcionario",
                "🗑️ Eliminar Actividad",
                "✏️ Modificar Actividad",
                "📄 Historial de Modificaciones",
                "🆕 Crear Nueva Cuenta",
                "📤 Cargar actividades Excel"
            ])

        # ----- ADMIN -----
        elif st.session_state.rol == "Administrador":
            opcion = st.sidebar.radio("Selecciona una opción:", [
                "🏠 Inicio",
                "🔍 Ver Carga de Trabajo por ID",
                "📋 Ver Todas las Cargas de Trabajo",
                "➕ Detalles de Periodo de Estudio",
                "📝 Agregar Actividad",
                "❌ Eliminar Funcionario",
                "🗑️ Eliminar Actividad",
                "✏️ Modificar Actividad",
                "📄 Historial de Modificaciones",
                "🆕 Crear Nueva Cuenta"
            ])

        # ----- USUARIO -----
        elif st.session_state.rol == "Usuario":
            opcion = st.sidebar.radio("Selecciona una opción:", [
                "🏠 Inicio",
                "➕ Detalles de Periodo de EstudioU",
                "📝 Agregar ActividadU",
                "✏️ Modificar ActividadU",
                "🗑️ Eliminar ActividadU",
                "📤 Cargar actividades desde Excel"
            ])
        else:
            st.error("Rol no reconocido.")
            return

        # ---------------- FUNCIONES PARA TODOS LOS ROLES ----------------
        if opcion == "🏠 Inicio":
            home()
        elif opcion == "🔍 Ver Carga de Trabajo por ID":
            ver_carga_trabajo()
        elif opcion == "📋 Ver Todas las Cargas de Trabajo":
            ver_todas_cargas_trabajo()
        elif opcion == "➕ Detalles de Periodo de Estudio":
            agregar_funcionario()
        elif opcion == "📝 Agregar Actividad":
            agregar_actividad()
        elif opcion == "❌ Eliminar Funcionario":
            eliminar_funcionario()
        elif opcion == "🗑️ Eliminar Actividad":
            eliminar_actividad()
        elif opcion == "✏️ Modificar Actividad":
            modificar_actividad()
        elif opcion == "📄 Historial de Modificaciones":
            mostrar_historial_modificaciones()
        elif opcion == "🆕 Crear Nueva Cuenta":
            crear_cuenta()
        elif opcion == "➕ Detalles de Periodo de EstudioU":
            agregar_funcionarioU()
        elif opcion == "📝 Agregar ActividadU":
            agregar_actividadU(st.session_state.usuario_id)
        elif opcion == "✏️ Modificar ActividadU":
            modificar_actividadU()
        elif opcion == "🗑️ Eliminar ActividadU":
            eliminar_actividadU()
        elif opcion == "📤 Cargar actividades desde Excel":
            cargar_actividades_excel(st.session_state.usuario_id)
        elif opcion == "📤 Cargar actividades Excel":
            cargar_actividades_excell()



# Función principal
menu_principal()
