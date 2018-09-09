from flask import Flask, render_template,session,redirect,url_for,request, send_file
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import FileField, SubmitField,StringField, SelectField
from wtforms.validators import DataRequired
import csv
from flask_uploads import UploadSet,configure_uploads, DATA
from record_matching import urbantide_record_matching, evaluator
import tablib
import boto3
import aws_connector
import os

# Sets up Amazon Web Services connector
BUCKET_NAME = 'usmart-exploratory-datascience'
session = boto3.Session(
    aws_access_key_id=aws_connector.aws_id,
    aws_secret_access_key=aws_connector.aws_pass
)
s3 = session.resource('s3')
# Flask app init and bootstrap
app = Flask(__name__)
bootstrap = Bootstrap(app)
# Configures local folder
dataset = UploadSet('dataset',DATA)
app.config['UPLOADED_DATASET_DEST'] = 'static/'
configure_uploads(app,dataset)
# Flask support for displaying csv files on html
final = tablib.Dataset()
# Forms 
class FileForm(FlaskForm):
    csv = FileField('Upload dataset to be matched', validators=[DataRequired()])
    submit = SubmitField('Submit')

class ColumnForm(FlaskForm):
    column = SelectField('Select column name',coerce=str)
    submit = SubmitField('Submit')

class PublicDataForm(FlaskForm):
    public_csv = SelectField('Select public dataset to match against your data',coerce=str)
    submit = SubmitField('Submit')

class PublicColumnForm(FlaskForm):
    public_column = SelectField('Select column name',coerce=str)
    submit = SubmitField('Submit')

class TestingForm(FlaskForm):
    test_csv = FileField('Upload ground-truth dataset', validators=[DataRequired()])
    submit = SubmitField('Submit')

class TestingColumnForm(FlaskForm):
    test_column = SelectField('Select ground-truth column', coerce=str)
    submit = SubmitField('Submit')

class TestResultForm(FlaskForm):
    eval_type = SelectField('Select classification method', coerce=str)
    submit = SubmitField('Submit')

# Web app
@app.route('/',methods=['GET','POST'])
def index():
    form = FileForm()
    if request.method == 'POST' and 'csv' in request.files:
        csvfile = dataset.save(request.files['csv'])
        return redirect(url_for('choose_column',csvfile=csvfile))
    return render_template('index.html',form=form, which=0)

@app.route('/col',methods=['GET','POST'])
def choose_column():
    column_form = ColumnForm()
    csvfile = request.args['csvfile']
    data_file = open('static/'+csvfile)
    csv_file = csv.reader(data_file)
    for row in csv_file:
        column_form.column.choices = [(i,i) for i in row]
        break
    if request.method == 'POST':
        column = request.form['column']
        return redirect(url_for('choose_public_dataset',csvfile=csvfile,column=column))
    return render_template('index.html',column_form=column_form,which=1)

@app.route('/public_df',methods=['GET','POST'])
def choose_public_dataset():
    csvfile = request.args['csvfile']
    column = request.args['column']
    public_form = PublicDataForm()
    public_form.public_csv.choices = [('OSCR','Office of the Scottish Charity Regulator'),('Charities_Regulator','UK Charities'),('Companies_House','Companies House Registrar'),('public_sector_names','Public Sector Authorities in Scotland')]
    if request.method == 'POST':
        public_dataset = request.form['public_csv']
        return redirect(url_for('choose_public_column',csvfile=csvfile,column=column,public_dataset=public_dataset))
    return render_template('index.html',public_form=public_form,which=2)

@app.route('/col_public',methods=['GET','POST'])
def choose_public_column():
    public_column_form = PublicColumnForm()
    csvfile = request.args['csvfile']
    column = request.args['column']
    public_dataset = request.args['public_dataset']
    try:
        data_file = open(public_dataset+'.csv')
    except:
        s3.Bucket(BUCKET_NAME).download_file('NLC/source_data/'+public_dataset+'.csv',public_dataset+'.csv')
        data_file = open(public_dataset+'.csv')
    
    csv_file = csv.reader(data_file)
    for row in csv_file:
        public_column_form.public_column.choices = [(i,i) for i in row]
        break
    if request.method == 'POST':
        public_column = request.form['public_column']
        result = urbantide_record_matching(csvfile,column,public_dataset,public_column,0)
        result[0].to_csv('./static/full_results.csv',index=False)
        result[1].to_csv('./static/results.csv',index=False)

        results_file = open('static/results.csv')
        final.csv = results_file.read()
        matches = final.html

        if os.environ['FLASK_ENV'] == 'development':
            testing = 1
        else:
            testing = 0
        
        return render_template('matches.html',matches=matches,testing=testing)
    return render_template('index.html',public_column_form=public_column_form,which=3)

@app.route('/full_matches',methods=['GET','POST'])
def show_all_matches():
    results_full_file = open('./static/full_results.csv')
    final.csv = results_full_file.read()
    full_matches = final.html
    return render_template('full_matches.html',full_matches=full_matches)

@app.route('/testing',methods=['GET','POST'])
def choose_truth_dataset():
    testing_form = TestingForm()
    if request.method == 'POST' and 'test_csv' in request.files:
        test_csv = dataset.save(request.files['test_csv'])
        return redirect(url_for('choose_truth_column_dataset',test_csv=test_csv))
    return render_template('index.html',testing_form=testing_form, which=4)

@app.route('/testing_column',methods=['GET','POST'])
def choose_truth_column_dataset():
    testing_column_form = TestingColumnForm()
    ground_truth_file = request.args['test_csv']
    try:
        test_file = open('static/'+ground_truth_file)
        csv_test_file = csv.reader(test_file)
        for row in csv_test_file:
            testing_column_form.test_column.choices = [(i,i) for i in row]
            break
    except:
        test_file = open('static/'+ground_truth_file,encoding="ISO-8859-1")
        csv_test_file = csv.reader(test_file)
        for row in csv_test_file:
            testing_column_form.test_column.choices = [(i,i) for i in row]
            break
    if request.method == 'POST':
        ground_truth_column = request.form['test_column']
        counts = evaluator(ground_truth_file,ground_truth_column)
        return redirect(url_for('displaying_results',counts=counts))
    return render_template('index.html',testing_column_form=testing_column_form,which=5)

@app.route('/eval',methods=['GET','POST'])
def displaying_results():
    test_result_form = TestResultForm()
    counts = request.args['counts']
    test_result_form.eval_type.choices = [('true_positives.csv','True Positives'+' - '+counts.split(',')[0]+' matches'),('false_positives.csv','False Positives'+' - '+counts.split(',')[1]+' matches')]
    precision = round(int(counts.split(',')[0]) / (int(counts.split(',')[0]) + int(counts.split(',')[1])),2)
    if request.method == 'POST':
        eval_choice = request.form['eval_type']
        eval_df = open('static/'+eval_choice)
        final.csv = eval_df.read()
        matches = final.html
        return render_template('eval.html',matches=matches)
    return render_template('index.html',test_result_form=test_result_form,precision=precision,which=6)
app.config['SECRET_KEY'] = 'lol'

@app.route('/downloadCSV')
def download_file():
    return send_file('static/results.csv',
                    mimetype='text/csv',
                    attachment_filename='results.csv',
                    as_attachment=True)

@app.route('/downloadFULLCSV')
def download_full_file():
    return send_file('static/full_results.csv',
                    mimetype='text/csv',
                    attachment_filename='full_results.csv',
                    as_attachment=True)    

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0')