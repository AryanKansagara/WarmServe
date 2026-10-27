"""Measure real local process cold/warm requests and capacity behavior."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
from urllib.request import Request, urlopen


def get(base,path):
    with urlopen(base+path,timeout=45) as response: return json.load(response)


def request(base,model):
    body=json.dumps({'model':model,'messages':[{'role':'user','content':'hello infrastructure'}],'max_tokens':8}).encode()
    start=time.perf_counter()
    with urlopen(Request(base+'/v1/chat/completions',data=body,headers={'Content-Type':'application/json'}),timeout=45) as response:
        result=json.load(response)
        assert result['model']==model and len(result['choices'])==1
        return {'milliseconds':(time.perf_counter()-start)*1000,'cold':response.headers['X-WarmServe-Cold']=='true'}


def main():
    root=Path(__file__).resolve().parent; artifacts=root/'artifacts';artifacts.mkdir(exist_ok=True)
    executable=artifacts/'warmserve'
    subprocess.run(['go','build','-o',str(executable),'.'],cwd=root,check=True,timeout=120)
    environment={**os.environ,'WARMSERVE_PYTHON':sys.executable,'WARMSERVE_WORKER':str(root/'worker.py'),'WARMSERVE_ADDR':'127.0.0.1:18088','WARMSERVE_IDLE_TTL':'4s'}
    log=(artifacts/'server.log').open('w')
    process=subprocess.Popen([str(executable)],cwd=root,env=environment,stdout=log,stderr=log)
    base='http://127.0.0.1:18088'
    try:
        for _ in range(100):
            try: get(base,'/healthz');break
            except OSError:
                if process.poll() is not None: raise RuntimeError('gateway failed startup')
                time.sleep(0.05)
        else: raise RuntimeError('gateway readiness timeout')
        cold=request(base,'demo-a')
        warm=[request(base,'demo-a') for _ in range(10)]
        assert cold['cold'] and not any(row['cold'] for row in warm)
        request(base,'demo-b');request(base,'demo-c')
        with ThreadPoolExecutor(max_workers=12) as executor:
            concurrent=list(executor.map(lambda index:request(base,'demo-b' if index%2 else 'demo-c'),range(12)))
        before=get(base,'/metrics')
        assert before['workers']<=2 and before['evictions']>=1
        for _ in range(120):
            after=get(base,'/metrics')
            if after['workers']==0:break
            time.sleep(0.05)
        else:raise RuntimeError('idle worker reclamation failed')
        report={'execution':'local CPU, real Python/PyTorch worker processes; not containers or GPUs',
                'cold_request_ms':cold['milliseconds'],'warm_requests':len(warm),
                'warm_median_ms':statistics.median(row['milliseconds'] for row in warm),
                'warm_p95_nearest_rank_ms':sorted(row['milliseconds'] for row in warm)[-1],
                'concurrent_requests_succeeded':len(concurrent),'worker_limit':2,
                'metrics_before_idle_reclaim':before,'metrics_after_idle_reclaim':after,
                'warning':'Tiny randomly initialized text model, not a pretrained LLM. Ten warm observations are not a production latency study.'}
        (artifacts/'benchmark.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2))
    finally:
        process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
        log.close()


if __name__=='__main__':main()
