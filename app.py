import os
from datetime import datetime

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from werkzeug.utils import secure_filename
import boto3
from .config import BUCKET, ACCESS_KEY_ID, ACCESS_SECRET_KEY, PG_USERNAME, PG_PASSWORD


app = Flask(__name__)
pg_instance = 'database-1.c9qr8s7d744i.eu-north-1.rds.amazonaws.com:5433'
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{PG_USERNAME}:{PG_PASSWORD}@{pg_instance}:5433/image_metadata'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key= ACCESS_SECRET_KEY, 
    region_name = 'eu-west-3',
)


class ImageModel(db.Model):
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
def hello():
    return {"hello": "world"}


@app.route('/upload', methods=['POST'])
def upload():
    if request.method == 'POST':
        img = request.files['file']
        if img:
            filename = secure_filename(img.filename)
            name, extention = filename.split('.')
            file_path = os.path.join('image_upload', filename)
            s3.upload_fileobj(Fileobj=img, Bucket=BUCKET, Key=file_path)

            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(BUCKET)
            for img_obj in bucket.objects.filter(Prefix=file_path):
                filesize = img_obj.size

            new_image = ImageModel(name=name, size=filesize, extention=extention)
            db.session.add(new_image)
            db.session.commit()
            return {"message": f"Image {new_image.name} has been downloaded successfully."}
        else:
            return {"error": "The request payload is not in JSON format"}


@app.route('/show', methods=['GET'])
def show_metadata():
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


@app.route('/delete', methods=['GET'])
def delete():
    if request.method == 'GET':
        if request.is_json:
            filename = request.get_json()['name']
            images = ImageModel.query.filter_by(name=filename)
            for img in images:
                db.session.delete(img)
                db.session.commit()
                bucket_folder = os.path.join('image_upload', img.name + '.' + img.extention)
                s3_resource = boto3.resource('s3')
                image_to_delete = s3_resource.Object(BUCKET, bucket_folder)
                image_to_delete.delete()
            return {"message": "Image has been deleted successfully."}
        else:
            return {"error": "The request payload is not in JSON format"}


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
