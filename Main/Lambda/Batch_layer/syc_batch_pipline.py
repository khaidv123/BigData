import sys
import os

# Thêm thư mục gốc vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Batch_layer.batch_layer import batch_layer

while True:
    print("begin")
    batch_layer()
    
