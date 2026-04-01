"""
Entrypoint que decide o que rodar baseado na variável MODE.
MODE=backfill → roda backfill.py
MODE=api (default) → roda uvicorn (API)
"""
import os
import sys

mode = os.environ.get("MODE", "api")

if mode == "backfill":
    import backfill
    backfill.main()
else:
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
