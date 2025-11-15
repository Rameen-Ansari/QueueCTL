QueueCTL — Backend Developer Internship Assignment

Completed core features and two bonus features (job output logging, metrics)

---
Contents: 

queuectl.py — CLI entrypoint (enqueue, list, status, config, worker, dlq, logs, stats)  
db.py — SQLite persistence (queuectl.db)  
worker.py — Worker engine (start/stop, job claiming, retries, backoff)  
test_flow.ps1 — Quick Windows PowerShell test script  
README.md — (this file)  
logs — runtime directory with job output logs (auto-created)  
sample screenshots — screenshots showcasing executions of various commands

---

Usage :

Open two terminals (Terminal A for workers, Terminal B for CLI/test)

Terminal A — start workers 
python .\queuectl.py worker start --count 2

Terminal B — enqueue jobs and inspect
python .\queuectl.py enqueue --command "echo hello"
python .\queuectl.py enqueue --command 'cmd /c exit 1'   
python .\queuectl.py list 
python .\queuectl.py status
python .\queuectl.py dlq list
python .\queuectl.py dlq retry --job-id <jobid>
python .\queuectl.py logs --job-id <jobid>   
python .\queuectl.py stats      

Test script (automates the above)

In Terminal B:

.\test_flow.ps1

---

CLI reference

queuectl enqueue --command "<cmd>" [--id <id>] [--max-retries N]

queuectl list [--state pending|processing|completed|dead] [--limit N]

queuectl status

queuectl config --set <KEY> <VALUE> 

queuectl worker start --count N

queuectl worker stop

queuectl dlq list

queuectl dlq retry --job-id <id>

queuectl logs --job-id <id>


queuectl stats
