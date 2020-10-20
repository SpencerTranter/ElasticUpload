const { Client } = require('@elastic/elasticsearch')
const through2 = require('through2')
const split2 = require('split2')
const batch2 = require('batch2')
const relaxedJson = require('really-relaxed-json')
const stream = require('stream')
const fs = require('fs')

const client = new Client({
  cloud: {
    id: ''
  },
  auth: {
    username: '',
    password: ''
  }
});

const index = 'log-kofi-test'

const upload = async () => {
  console.log(index)

  const writeToES = () =>
    new Promise((resolve, reject) => {
      const read = fs.createReadStream('input/test_log.json')

      read.on('error', (err) => {
        reject(err)
      })

      read
        .pipe(split2())
        .pipe(batch2({ size: 2000 }))
        .pipe(docTransformStream())
        .pipe(bulkWriteStream(client))
        .on('finish', () => {
          resolve('Success')
        })
        .on('error', (err) => {
          console.log('Last error')
          console.log(err)
          reject(err)
        })
    })

  try {
    const result = await writeToES()
    console.log('result', result)
  } catch (err) {
    console.log('Error: ', err)
  }
}

const docTransformStream = () => through2.obj((data, enc, callback) => {
  const jsonArrayString = relaxedJson.toJson('[' + data.toString() + ']')
  const jsonArray = JSON.parse(jsonArrayString)
  const response = jsonArray.reduce((acc, curr, idx) => {
    acc.push({index: {_index: index, pipeline: `pipeline-develop-1`}})
    acc.push(curr)

    // Parallel calls to bulk?
    // if (idx < 100) {
    //   acc[0].push({index: {_index: index, pipeline: `pipeline-develop-1`}})
    //   acc[0].push(curr)
    // } else {
    //   acc[1].push({index: {_index: index, pipeline: `pipeline-develop-1`}})
    //   acc[1].push(curr)
    // }
    return acc
  }, [])
  return callback(null, response)
})

const bulkWriteStream = (client) => {
  let count = 0
  const writeStream = new stream.Writable({
    objectMode: true,
    highWaterMark: 1
  })

  writeStream._write = (data, encoding, callback) => {
    count += 2000
    console.log('upload', count)

    client.bulk({ body: data }, callback)

    // Parallel calls to bulk?
    // client.bulk({ body: data[0] }, callback)
    // if (data[1].length > 0)
    // client.bulk({ body: data[1] })
  }

  return writeStream
}

upload().then(console.log).catch(console.log)
