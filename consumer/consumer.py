import datetime
from flask import Flask, Response, render_template, redirect, jsonify
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
topic = "javainuse-topic"
topic2= "javainuse-topic2"
consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=['localhost:9092'])

consumer2 = KafkaConsumer(
    topic2, 
    bootstrap_servers=['localhost:9092'])

# Set the consumer in a Flask App
app = Flask(__name__)

crime = 0
Flag=False
@app.route('/')
def index():
    #print(crime)
    return render_template('index.html',result = 0)

#Route for video feed 1 from kafka topic "javainuse-topic", running producer.py code
@app.route('/video_feed', methods=['GET'])
def video_feed():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')
#Route for video feed 2 from kafka topic "javainuse-topic2", running producer2.py code
@app.route('/video_feed2', methods=['GET'])
def video_feed2():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream2(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')


def get_video_stream():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        #val will be the last string value from the bytes given by the producer, which will determine whether anomaly is detected or not
        # 0 - no anomaly detected
        # 1- Anomaly detected
        val=int.from_bytes(msg.value[-1:], byteorder='big', signed=False)
        #global fnal_val
        if(val==1):
            print("Camera 1")
            print("Anomaly Detected")
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
def get_video_stream2():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer2:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

if __name__ == "__main__":
    app.run()
