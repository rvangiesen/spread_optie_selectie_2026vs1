import os

def fix_ib_client():
    path = r'c:\Users\Gebruiker\Documents\Python_Projecten\AntiGravity Project 2_ spreadselectie_ setup via AG\ib_client.py'
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 1. Fix reqContractDetailsAsync coroutine error
    # Old: task = loop.create_task(self.ib.reqContractDetailsAsync(pattern))
    # New: task = asyncio.ensure_future(self.ib.reqContractDetailsAsync(pattern))
    old_task = "task = loop.create_task(self.ib.reqContractDetailsAsync(pattern))"
    new_task = "task = asyncio.ensure_future(self.ib.reqContractDetailsAsync(pattern))"
    content = content.replace(old_task, new_task)
    
    # 2. Increase historical data timeout from 3.0 to 12.0
    old_timeout = "t.join(timeout=3.0) # 3 seconds max"
    new_timeout = "t.join(timeout=12.0) # 12 seconds max"
    content = content.replace(old_timeout, new_timeout)
    
    # Also handle variants if any
    content = content.replace("t.join(timeout=3.0)", "t.join(timeout=12.0)")

    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("Fixed ib_client.py")

if __name__ == "__main__":
    fix_ib_client()
