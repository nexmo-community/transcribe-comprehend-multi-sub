import sys
import asyncio
import aiofile

from amazon_transcribe.client import TranscribeStreamingClient
from amazon_transcribe.handlers import TranscriptResultStreamHandler
from amazon_transcribe.model import TranscriptEvent

class MyEventHandler(TranscriptResultStreamHandler):

    def __init__(self, *args):
        super().__init__(*args)
        self.result_holder = []

    async def handle_transcript_event(self, transcript_event: TranscriptEvent):
        results = transcript_event.transcript.results
        for result in results:
            for alt in result.alternatives:
                self.result_holder.append(alt.transcript)
                # print(alt.transcript)

#----------

async def basic_transcribe(file, media_sample_rate_hz=8000, language_code="en-US", region="us-east-1"):

    # Setup up our client with our chosen AWS region
    client = TranscribeStreamingClient(region=region)

    # Start transcription to generate our async stream
    stream = await client.start_stream_transcription(
        language_code=language_code,
        media_sample_rate_hz=media_sample_rate_hz,
        media_encoding="pcm",
    )

    async def write_chunks():

        async with aiofile.AIOFile(file, 'rb') as afp:
            reader = aiofile.Reader(afp, chunk_size=media_sample_rate_hz * 1.024)
            async for chunk in reader:
                await stream.input_stream.send_audio_event(audio_chunk=chunk)
     
        await stream.input_stream.end_stream()

    # Instantiate our handler and start processing events
    handler = MyEventHandler(stream.output_stream)

    await asyncio.gather(write_chunks(), handler.handle_events())

    if handler.result_holder == [] :
        return('')
    else :    
        return (handler.result_holder[-1])

#------------------        

fn = sys.argv[1]    # audio file name
lc = sys.argv[2]    # language code, e.g. en-US
r = sys.argv[3]     # region, e.g. us-east-1

loop = asyncio.get_event_loop()
transcript = loop.run_until_complete(asyncio.gather(basic_transcribe(file=fn, language_code=lc, region=r)))[-1]
loop.close()

print(transcript)