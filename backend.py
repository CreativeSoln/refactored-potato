from fastapi import FastAPI
import uvicorn
import json
import os
import subprocess
import sys
from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent
FILE_PATH = BASE_DIR / "ecu_diagnostic_structure_combined.json"

print("CWD =", os.getcwd())


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/load_dids")
async def load_dids(pdx: str):
    
    try:
        if not pdx:
            return {"status": "error", "message": "Missing pdx parameter"}

        req = {
            "action": "parse_pdx",
            "pdx_path": pdx
        }

        proc = subprocess.Popen(
            [sys.executable, str(BASE_DIR / "cli.py")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = proc.communicate(json.dumps(req) + "\n")

        if stderr:
            return {"status": "error", "message": stderr}

        result = json.loads(stdout)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}

    
    # try:
    #     with open(FILE_PATH, "r") as f:
    #         return json.load(f)
    # except Exception as e:
    #     import traceback
    #     traceback.print_exc()
    #     return {"error": str(e)}

@app.get("/health")
async def health():
    return {
        "status": "OK",
        "message": "Python Service Running"
    }

@app.post("/decode")
async def decode(did: str, hexData: str):
    # TODO: plug real decode
    return {
        "did": did,
        "decoded": "simulation placeholder",
        "raw": hexData
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5015)
