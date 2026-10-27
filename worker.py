"""Small real PyTorch text-inference worker; not a pretrained LLM."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import torch
from torch import nn


class TinyText(nn.Module):
    def __init__(self):
        super().__init__()
        self.embedding = nn.Embedding(128, 16)
        self.gru = nn.GRU(16, 16, batch_first=True)
        self.output = nn.Linear(16, 128)

    def forward(self, token, hidden=None):
        values, hidden = self.gru(self.embedding(token), hidden)
        return self.output(values), hidden


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', choices=['demo-a', 'demo-b', 'demo-c'], required=True)
    args = parser.parse_args()
    torch.set_num_threads(1); torch.manual_seed({'demo-a':1,'demo-b':2,'demo-c':3}[args.model])
    model = TinyText().eval()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass

        def send(self, status, data):
            encoded = json.dumps(data).encode()
            self.send_response(status); self.send_header('Content-Type', 'application/json'); self.send_header('Content-Length', str(len(encoded))); self.end_headers(); self.wfile.write(encoded)

        def do_GET(self):
            self.send(200 if self.path == '/healthz' else 404, {'model': args.model, 'status':'ready'})

        def do_POST(self):
            if self.path != '/v1/chat/completions': self.send(404, {'error':'not_found'}); return
            try:
                length = int(self.headers.get('Content-Length','0'))
                if not 0 < length <= 16384: raise ValueError('invalid body size')
                data = json.loads(self.rfile.read(length))
                prompt = data['messages'][-1]['content']
                maximum = data.get('max_tokens',8)
                if data.get('model') != args.model or not isinstance(prompt,str) or not 1 <= len(prompt.encode()) <= 128 or type(maximum) is not int or not 1 <= maximum <= 16: raise ValueError('invalid prompt or generation bounds')
                tokens = torch.tensor([[byte % 128 for byte in prompt.encode()]], dtype=torch.long)
                generated = []
                with torch.inference_mode():
                    logits, hidden = model(tokens)
                    for _ in range(maximum):
                        value = int(logits[0,-1].argmax()); generated.append(value)
                        logits, hidden = model(torch.tensor([[value]]), hidden)
                content = ''.join(chr(value) if 32 <= value < 127 else '?' for value in generated)
                self.send(200, {'model':args.model,'choices':[{'index':0,'message':{'role':'assistant','content':content},'finish_reason':'length'}], 'usage':{'prompt_tokens':tokens.shape[1],'completion_tokens':maximum}})
            except (ValueError, KeyError, IndexError, TypeError):
                self.send(400, {'error':'invalid_request'})

    server = HTTPServer(('127.0.0.1',0),Handler)
    print(json.dumps({'url':f'http://127.0.0.1:{server.server_port}'}),flush=True)
    server.serve_forever()


if __name__ == '__main__': main()
