# Amazon Transcribe & Comprehend connector

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/nexmo-se/transcribe-comprehend-multi-sub)

Use this Transcribe & Comprehend connector for real time transcription and sentiment analysis of voice calls.

## Amazon Transcribe & Comprehend connector

In order to get started, you will need to have an [AWS account](http://aws.amazon.com).

You will also need to know an active AWS Access Key ID and Secret Key pair.

If necessary, create a new pair of keys:
- Log in to your [AWS Management Console](http://aws.amazon.com).
- Click on your user name at the top right of the page.
- Click on the My Security Credentials link from the drop-down menu.
- Go to Access keys section,
- Click on \[Create New Access Key\] (\*)

(\*) *Note: Your AWS account may be limited to only 2 active Access Keys. To create a new pair of Keys, you may need to "Make Inactive" an existing active Access Key ID, however before doing so, you need to absolutely make sure that key is not used by your other applications.*

## About this connector

Vonage Voice API's Amazon Transcribe & Comprehend connector makes use of the [WebSockets feature](https://docs.nexmo.com/voice/voice-api/websockets). When a call is established, your Vonage Voice API application makes a websocket connection to this connector and streams the audio in real time via the websocket.

The connector posts back in real time transcripts and optionally sentiment scores, via a webhook call back to your Vonage Voice API application. It is a multi-threaded server application with subprocesses to avoid unnecessary long idle https sessions to AWS after transcription and sentiment analysis requests.

See https://github.com/nexmo-se/transcribe-comprehend-client for a sample code on how an application using Vonage Voice API can use the connector for real time transcription and sentiment analysis of voice calls.

The parameter `sensitivity` allows the Voice API application to set the VAD (Voice Activity Detection) sensitivity from the most sensitive (value = 0) to the least sensitive (value = 3), this is an integer value.

## Running Transcibe and Comprehend connector

You may select one of the following 4 types of deployments.

### Docker deployment

Copy the `.env.example` file over to a new file called `.env`:
```bash
cp .env.example .env
```

Edit `.env` file,<br/>
set the 3 first parameters with their respective values retrieved from your AWS account,<br/>
set the `PORT` value (e.g. *5000*) where websockets connections will be established.
The `PORT` value needs to be the same as specified in `Dockerfile` and `docker-compose.yml` files.

Launch the Transcribe & Comprehend connector as a Docker instance:

```bash
docker-compose up
```
Your Docker container's public hostname and port will be used by your Vonage Voice API application as part of the websocket uri `wss://<docker_host_name>:<proxy_port>`, e.g. *myserver.mycompany.com:40000*, or *xxxxx.ngrok.io*.

### Local deployment

To run your own instance locally you'll need an up-to-date version of Python 3.8 (we tested with version 3.8.5).

Copy the `.env.example` file over to a new file called `.env`:

```bash
cp .env.example .env
```

Edit `.env` file,<br/>
set the 3 first parameters with their respective values retrieved from your AWS account,<br/>
set the `PORT` value where websockets connections will be established.

Install dependencies once:
```bash
pip install --upgrade -r requirements.txt
```

Launch the connector service:
```bash
python transcribe-comprehend-multi-sub.py
```

Your server's public hostname and port will be used by your Vonage Voice API application as part of the websocket uri `wss://<serverhostname>:<port>`, e.g. `wss://abcdef123456.ngrok.io`


Specifically with the sample application https://github.com/nexmo-se/transcribe-comprehend-client, you will set TRANSCRIBE_COMPREHEND_CONNECTOR_SERVER argument as for example `abcdef123456.ngrok.io`.


### Command Line Heroku deployment

Install [git](https://git-scm.com/downloads).

Install [Heroku command line](https://devcenter.heroku.com/categories/command-line) and login to your Heroku account.

Download this sample application code to a local folder, then go to that folder.

If you do not yet have a local git repository, create one:</br>
```bash
git init
git add .
git commit -am "initial"
```

Deploy this connector application to Heroku from the command line using the Heroku CLI:

```bash
heroku create myappname
```

On your Heroku dashboard where your connector application page is shown, click on `Settings` button,
add the following `Config Vars` and set them with their respective values:</br>
AWS_ACCESS_KEY_ID</br>
AWS_DEFAULT_REGION</br>
AWS_SECRET_ACCESS_KEY</br>

```bash
git push heroku master
```

On your Heroku dashboard where your connector application page is shown, click on `Open App` button, that URL will be the one to be used by your Vonage Voice API application as part of the websocket uri, e.g. `wss://myappname.herokuapp.com`

Specifically with the sample application https://github.com/nexmo-se/transcribe-comprehend-client, you will set TRANSCRIBE_COMPREHEND_CONNECTOR_SERVER argument as `myappname.herokuapp.com`

### 1-click Heroku deployment

Click the 'Deploy to Heroku' button at the top of this page, and follow the instructions to enter your Heroku application name and the 3 AWS parameter respective values retrieved from your AWS account.

Once deployed, on the Heroku dashboard where your connector application page is shown, click on `Open App` button, that URL will be the one to be used by your Vonage Voice API application as part of the websocket uri, e.g. `wss://myappname.herokuapp.com`.

Specifically with the sample application https://github.com/nexmo-se/transcribe-comprehend-client, you will set TRANSCRIBE_COMPREHEND_CONNECTOR_SERVER argument as `myappname.herokuapp.com`

## Usage capacity

This connector is a multi-threaded application that submits concurrent transcription requests to Amazon Transcribe in parallel.

With this reference code, one connected websocket corresponds to one concurrent transcription request. You may decide to update the code on your own to use queues and worker threads to serialize transcription requests from multiple connected websockets.

Make sure your voice application and connector application do not submit more than the maximum allowed (default = 5) concurrent transcription requests on your Amazon Transcribe account.

You may see more information on that subject [here](https://docs.aws.amazon.com/transcribe/latest/dg/limits-guidelines.html).
