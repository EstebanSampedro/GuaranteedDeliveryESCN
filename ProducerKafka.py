from flask import Flask, request
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from confluent_kafka import Producer
import threading
import time

app = Flask(__name__)

# Configuración de la base de datos MySQL
username = 'root'  # Asegúrate de cambiar esto por tu usuario real
password = 'root'  # Cambia esto por tu contraseña real
host = 'localhost'  # o la dirección de tu servidor MySQL
dbname = 'gdelivery'  # El nombre de tu base de datos
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{dbname}')

Base = declarative_base()

# Modelo de datos
class Message(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    message = Column(String, nullable=False)
    sent = Column(Boolean, default=False)
    

# Es importante llamar a Base.metadata.create_all después de definir los modelos
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def send_message_to_kafka(message):
        kafka_config = {'bootstrap.servers': 'localhost:9092'}  # Ajusta a tu configuración de Kafka
        kafka_producer = Producer(**kafka_config)
        kafka_producer.produce('test', value=message)
        kafka_producer.poll(0)  # Poll to trigger delivery callbacks
        estado_productor = kafka_producer.flush(timeout=10)
        if estado_productor == 0:
            print("Correcto se envio el mensaje")
            return True
        elif estado_productor > 0:
            # Algunos mensajes no se pudieron entregar, manejar según sea necesario
            print(f"{estado_productor} mensajes no entregados")
            guardar_en_base_de_datos(message)
            print(f"Error al enviar a Kafka")
            return False


def guardar_en_base_de_datos(message):
    try:
        # Guarda el mensaje en la base de datos
        session = Session()
        # Corrección aquí: pasar argumentos con nombre en lugar de posicionales
        new_message = Message(message=message, topic='test', sent=False)
        session.add(new_message)
        session.commit()
    except SQLAlchemyError as e:
        print(f"Error al guardar en la base de datos: {e}")
        # Corrección: debe ser session.rollback() en lugar de session.session.rollback()
        session.rollback()
    finally:
        # Asegúrate de cerrar la sesión en un bloque finally para garantizar que se liberen los recursos
        session.close()



@app.route('/message', methods=['POST'])
def receive_message():
    mensaje = request.json['content']
    # Llama a la función para enviar el mensaje a Kafka
    send_message_to_kafka(mensaje)
    print(f"ESTE ES EL MENSAJE: {mensaje}")
    return 'Mensaje enviado a Kafka'

def message_sender():
    while True:
        session = Session()
        unsent_messages = session.query(Message).filter_by(sent=False).all()
        for message in unsent_messages:
            if send_message_to_kafka(message.message):
                message.sent = True
                session.commit()
        session.close()
        time.sleep(180)  # Espera 3 minutos antes de volver a intentar

# Inicia el thread de envío de mensajes
threading.Thread(target=message_sender, daemon=True).start()

if __name__ == '__main__':
    app.run(debug=True)