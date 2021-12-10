import sys
import base64
# python decoder.py './encoded_processed.txt' 'processed.csv'
# python decoder.py './encoded_correlation.txt' 'correlation.csv'

def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Read the input files
    with open(input_file) as f:
        data = f.read()

    code_dump = base64.b64decode(data)

    # Write out the CSV inputs
    with open(output_file, 'wb') as f1:
        f1.write(code_dump)

if __name__ == "__main__":
    main()
