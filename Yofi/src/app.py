from flask import Flask, render_template, request, flash, redirect, url_for
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import os

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY')

SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

@app.route('/')
def home():
    return render_template('index.html', year=datetime.now().year)

@app.route('/over')
def about():
    return render_template('over-ons.html', year=datetime.now().year)

@app.route('/werken-bij')
def jobs():
    return render_template('werken-bij.html', year=datetime.now().year)

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if request.method == 'POST':
        try:
            # Get form data
            name = request.form['name']
            telephone = request.form['telephone']
            email = request.form['email']
            website = request.form['website']
            message = request.form['message']
            
            # Format message for Slack
            slack_message = {
                "text": f"New Contact Form Submission\n"
                        f"*Name:* {name}\n"
                        f"*Telefoon:* {telephone}\n"
                        f"*Email:* {email}\n"
                        f"*Website:* {website}\n"
                        f"*Message:* {message}"
            }
            
            # Send to Slack
            response = requests.post(
                SLACK_WEBHOOK_URL,
                data=json.dumps(slack_message),
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                flash('Bericht succesvol verzonden!', 'success')
            else:
                flash('Er is iets misgegaan. Probeer het later opnieuw.', 'error')
                
            return redirect(url_for('contact'))
            
        except Exception as e:
            flash('Er is iets misgegaan. Probeer het later opnieuw.', 'error')
            return redirect(url_for('contact'))

    return render_template('contact.html', year=datetime.now().year)

@app.route('/privacy')
def privacy():
    return render_template('privacy.html', year=datetime.now().year)

if __name__ == '__main__':
    app.run(debug=True)
