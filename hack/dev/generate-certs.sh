# This script generates self signed certs to be used when running scheduler locally in DEVELOPMENT mode
# DO NOT USE THIS SCRIPT TO GENERATE CERTS FOR YOUR PRODUCTION SYSTEMS

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

TMP_DIR="${SCRIPT_DIR}/../../out/tmp/generated_keys"
mkdir -p $TMP_DIR

# Generate the root ca key
openssl genrsa -out "${TMP_DIR}/rootCA.key" 2048

# Generate the root ca cert from the key
openssl req -batch -x509 -sha256 -new -nodes -key ${TMP_DIR}/rootCA.key -days 3650 -out ${TMP_DIR}/rootCA.crt

# Generate the signing request for the witchcraft ssl cert as well as the private key
openssl req -new -nodes -newkey rsa:2048 -keyout ${TMP_DIR}/spark-scheduler.key -out ${TMP_DIR}/spark-scheduler.csr -config cert.conf -extensions 'v3_req'

# Sign the signing request with the root ca and generate the ssl certificate to use in the witchcraft server
openssl x509 -req -in ${TMP_DIR}/spark-scheduler.csr -CA ${TMP_DIR}/rootCA.crt -CAkey ${TMP_DIR}/rootCA.key -CAcreateserial -out ${TMP_DIR}/spark-scheduler.crt -days 3000 -sha256 -extfile cert.conf -extensions v3_req
