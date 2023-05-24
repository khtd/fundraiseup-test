import { argv } from 'node:process';
import { MongoClient, ChangeStream, ChangeStreamUpdateDocument, ChangeStreamInsertDocument, Collection } from 'mongodb';
import seedrandom from 'seedrandom'
import { cloneDeep } from 'lodash'
import { ICustomer } from './common/types/Customer';
import { DB_CUSTOMERS_COLLECTION_NAME, DB_CUSTOMERS_ANONYMISED_COLLECTION_NAME ,DB_NAME, SYNC_BATCH_FLUSH_TIMEOUT_MS, SUNC_BATCH_FLUSH_SIZE } from './common/constraints';
import { config } from './common/config'


class SyncApp {
  client: MongoClient = new MongoClient(config.dbUrl);
  changeStream: ChangeStream<ICustomer, ChangeStreamInsertDocument<ICustomer> | ChangeStreamUpdateDocument<ICustomer>>;
  inputCollection: Collection<ICustomer>
  outputCollection: Collection<ICustomer>

  batch: ICustomer[] = []

  async init() {
    await this.client.connect();
    
    const db = this.client.db(DB_NAME);

    this.inputCollection = db.collection<ICustomer>(DB_CUSTOMERS_COLLECTION_NAME);
    this.outputCollection = db.collection<ICustomer>(DB_CUSTOMERS_ANONYMISED_COLLECTION_NAME);
  }

  async start() {
    if(argv.includes('--full-reindex')) {
      await this.fullReindex()
    } else {
      this.subscribe()
      await this.retrieveLastUpdates()
    }
  }

  async shutdown(success: boolean = true) {
    console.log('exiting...');
    try {
      await this.changeStream?.close();
      await this.client?.close();
    } catch (err) {
      success = false;
      console.log('error during shutdown')
    } finally {
      process.exit(success ? 0 : 1);
    }
  }
  
  onError(error: Error) {
    console.log(error)
    this.shutdown(false)
  }

  private async fullReindex() {
    this.outputCollection.deleteMany({});

    var cursor = this.inputCollection.find();

    var data = [];
    while(await cursor.hasNext()){
      const customer = await cursor.next() as ICustomer
      data.push(this.anonymize(customer));

      if(data.length > 1000) await this.outputCollection.insertMany(data)
    }

    await this.outputCollection.insertMany(data)

    console.log('success')
    this.shutdown(true)
  } 

  private subscribe() {
    this.changeStream = this.inputCollection.watch([{ $match: { operationType: { $in: ['insert', 'update']}  } }], { fullDocument: "updateLookup" });
  
    this.changeStream.on("change", (change) => {
      this.batch.push(this.anonymize(change.fullDocument as ICustomer))

      if(this.batch.length === SUNC_BATCH_FLUSH_SIZE) {
        console.log('flush batch by size')
        this.processBatch()
      }
    });
  
    setTimeout(this.subscribe_timer_cb.bind(this), SYNC_BATCH_FLUSH_TIMEOUT_MS)
  }

  private subscribe_timer_cb() {
    console.log('flush batch by timer')
    this.processBatch()
    setTimeout(this.subscribe_timer_cb.bind(this), SYNC_BATCH_FLUSH_TIMEOUT_MS)
  }

  private async processBatch() {
    if(this.batch.length == 0) return;
    const toProcess = [...this.batch];
    this.batch = [];
  
    const exists = await this.outputCollection.find({ _id: {$in: toProcess.map(c => c._id )} }, {projection: { _id: 1 }}).toArray();
    const existsIds = exists.map(e => e._id);
    
    const toUpdate: ICustomer[] = []
    const toAdd: ICustomer[] = []
  
    for (let c of toProcess) {
      if (existsIds.includes(c._id)) toUpdate.push(c) 
      else toAdd.push(c)
    }
  
    for (let doc of toUpdate) {
      this.outputCollection.updateOne({ _id: doc._id }, doc)
    }
    this.outputCollection.insertMany(toAdd)
    console.log(`proccessed batch, size ${toProcess.length}`)
  }

  private async retrieveLastUpdates() {
    const lastAdded = await this.outputCollection.findOne({}, { sort: { createdAt: -1 } })
    const toAdd = await this.inputCollection.find({createdAt: { $gt: lastAdded?.createdAt }}).toArray()
    //in order to check *updated* documents we should add "updatedAt" column to the model.
  
    if(toAdd.length > 0) await this.outputCollection.insertMany(toAdd.map(this.anonymize.bind(this)));
  }
  
  private anonymize(customer: ICustomer) {
    let output: ICustomer = cloneDeep(customer);
  
    output.firstName = this.getRandomString(customer.firstName)
    output.lastName = this.getRandomString(customer.firstName)
  
    const [localPart, domain] = customer.email.split('@')
    output.email = `${this.getRandomString(localPart)}@${domain}`
  
    output.address.line1 = this.getRandomString(customer.firstName)
    output.address.line2 = this.getRandomString(customer.firstName)
    output.address.postcode = this.getRandomString(customer.address.postcode)
  
    return output;
  }

  private getRandomString(str: string) {
    var s = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var rng = seedrandom(str);
    return Array.from({length: 8}, () => s.charAt(Math.floor(rng() * s.length))).join('')
  }
}

const app = new SyncApp()
const onError = app.onError.bind(app)

process.on('uncaughtException', onError);
process.on('unhandledRejection', onError);
process.on('SIGINT', () => app.shutdown());

async function main() {
  await app.init()
  app.start()
}

main().catch(onError)