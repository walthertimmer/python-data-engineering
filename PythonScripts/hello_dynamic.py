import os
name = os.environ.get('NAME', 'NAME env not set!') 

print(f"Hello, {name}!")