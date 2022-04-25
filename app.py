import os
from datetime import datetime
import atexit
import boto3
from botocore.utils import IMDSFetcher

from flask import Flask, request, json
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from werkzeug.utils import secure_filename
from apscheduler.schedulers.background import BackgroundScheduler

from config import BUCKET, PG_USERNAME, PG_PASSWORD, SNS_TOPIC_ARN, SQS_URL
from utils import s3, sqs, sns, sqs_to_sns, invoke_lambda


# Check SQS for new messages every 2 minutes
cron = BackgroundScheduler(daemon=True)
cron.add_job(func=sqs_to_sns, trigger="interval", seconds=120)
# Explicitly kick off the background thread
cron.start()

# Shutdown your cron thread if the web process is stopped
atexit.register(lambda: cron.shutdown(wait=False))


app = Flask(__name__)
pg_instance = 'database-1.c9qr8s7d744i.eu-north-1.rds.amazonaws.com:5433'
#pg_instance = 'localhost:5433'
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{PG_USERNAME}:{PG_PASSWORD}@{pg_instance}/image_metadata'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)


class ImageModel(db.Model):
    """ Model for an image data"""
    __tablename__ = 'image_metadata'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    size = db.Column(db.String())
    extention = db.Column(db.String())
    uploaded = db.Column(db.DateTime, default=datetime.utcnow)

    def __init__(self, name, size, extention):
        self.name = name
        self.size = size
        self.extention = extention

    def __repr__(self):
        return f"<Image {self.name}>"


@app.route('/')
def get_instance():
    '''Endpoint to Return the name of the region and AZ the application is running in.'''
    region = IMDSFetcher()._get_request("/latest/meta-data/placement/region", None).text
    az = IMDSFetcher()._get_request("/latest/meta-data/placement/availability-zone", None).text
    return json.dumps({'region': region, 'az': az})


@app.route('/upload', methods=['POST'])
def upload():
    '''Endpoint to upload umages to S3 bucket, save image data in DB and 
    send message about it to SQS.'''
    if request.method == 'POST':
        img = request.files['file']
        if img:
            filename = secure_filename(img.filename)
            name, extention = filename.split('.')
            file_path = os.path.join('image_upload', filename)
            # img.save(file_path)
            # filesize = os.stat(file_path).st_size
            s3.upload_fileobj(Fileobj=img, Bucket=BUCKET, Key=file_path)
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(BUCKET)
            for img_obj in bucket.objects.filter(Prefix=file_path):
                filesize = img_obj.size

            new_image = ImageModel(name=name, size=filesize, extention=extention)
            db.session.add(new_image)
            db.session.commit()

            sns_msg = f'''
            An image {filename} has been uploaded to s3.
            Details:
            Name: {new_image.name} 
            Size: {new_image.size} bytes 
            Ext.: {new_image.extention} 
            Upd.: {new_image.uploaded} 

            To delete it, send POST request to endpoint:
            {request.url_root}delete

            The request must contain following JSON:
            "name": {new_image.name}
            '''
            sqs.send_message(QueueUrl=SQS_URL, MessageBody=sns_msg)
            print('New message has been published to SQS!')

            return {"message": f"Image {new_image.name} has been downloaded successfully."}
        else:
            return {"error": "The request payload is not in JSON format"}


@app.route('/show', methods=['GET'])
def show_metadata():
    '''Endpoint to show a list of uploaded images.'''
    images = ImageModel.query.all()
    results = [
        {
            "name": image.name,
            "size": image.size,
            "extention": image.extention,
            "uploaded_by": image.uploaded
        } 
        for image in images
    ]
    return {"count": len(results), "images": results, "message": "success"}


@app.route('/delete', methods=['POST'])
def delete():
    '''Endpoint to delete image from S3 bucket and DB'''
    if request.method == 'POST':
        if request.is_json:
            filename = request.get_json()['name']
            images = ImageModel.query.filter_by(name=filename)
            error_file_paths = []
            if not any([img for img in images]):
                return {"error": f"The file record does not exist in DB"}
            for img in images:
                db.session.delete(img)
                db.session.commit()
                file_path = os.path.join('image_upload', img.name + '.' + img.extention)
                # if os.path.exists(file_path):
                #     os.remove(file_path)
                # else:
                #     error_file_paths.append(file_path)
                s3_resource = boto3.resource('s3')
                image_to_delete = s3_resource.Object(BUCKET, file_path)
                image_to_delete.delete()
            if error_file_paths:
                return {"error": f"The file {error_file_paths} does not exist"}
            else:
                return {"message": "Image has been deleted successfully."}
        else:
            return {"error": "The request payload is not in JSON format"}


@app.route('/subscribe', methods=['POST'])
def subscribe():
    '''Endpoint to subscribe to SNS topic'''
    if request.method == 'POST':
        if request.is_json:
            email = request.get_json()['email']
            sns.subscribe(TopicArn=SNS_TOPIC_ARN,
                          Protocol='email',
                          Endpoint=email)
            msg = f'Subscription confirmation has been sent to {email}.' 
            return {"message": msg}


@app.route('/unsubscribe', methods=['POST'])
def unsubscribe():
    '''Endpoint for unsubscribe to SNS topic'''
    if request.method == 'POST':
        if request.is_json:
            email = request.get_json()['email']
            response = sns.list_subscriptions_by_topic(TopicArn=SNS_TOPIC_ARN)
            subscriptions = response["Subscriptions"]
            for sub in subscriptions:
                if sub['Endpoint'] == email:
                    sub_to_delete = sub['SubscriptionArn']
                    sns.unsubscribe(SubscriptionArn=sub_to_delete)
            return {"message": f'{email} has been unsubscribed!'}


@app.route('/lambda',methods=['GET'])
def trigger_lambda():
    '''Endpoint to trigger AWS lambda function'''
    invoke_lambda()
    return ('Lambda has been successfully invoked!')


if __name__ == '__main__':
    app.run(host="localhost", port=5000, debug=True)
