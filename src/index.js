import fs from 'fs'
import debug from 'debug'
import csvtojson from 'csvtojson'
import jsontocsv  from 'json-to-csv-stream'
import streamConcat from 'stream-concat'
import { dirname, join } from 'path'
import { pipeline, Transform } from 'stream'
import { promisify } from 'util'

const { pathname: currentFile } = new URL(import.meta.url)
const cwd = dirname(currentFile)
const filesDir = `${cwd}/dataset`
const output = `${cwd}/dataset/final.csv`
const logger = debug('app:concat')
const pipelineAsync = promisify(pipeline)
const memoryUsed = process.memoryUsage();

console.time('concat-data')

setInterval(() => process.stdout.write('.'), 1000).unref()

const files = (await  fs.promises.readdir(filesDir))
  .filter(item => !(!!~item.indexOf('.zip')))

const streams = files.map(item => fs.createReadStream(join(filesDir, item)))
const combinedStreams = new streamConcat(streams)
const finalStream = fs.createWriteStream(output)

const handleStream = new Transform({
  transform: (chunk, encoding, cb) => {
    const data = JSON.parse(chunk)

    const output = {
      id: data.Respondent,
      country: data.Country,
    }

    return cb(null, JSON.stringify(output))
  }
})

await pipelineAsync(
  combinedStreams,
  csvtojson(),
  handleStream,
  jsontocsv(),
  finalStream
) 

logger(`${files.length} files merged! On ${output}`)
console.timeEnd('concat-data')
