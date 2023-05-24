import { argv } from "node:process";
import {
  MongoClient,
  ChangeStream,
  ChangeStreamUpdateDocument,
  ChangeStreamInsertDocument,
  Collection,
  Filter,
  Db,
} from "mongodb";
import seedrandom from "seedrandom";
import { cloneDeep } from "lodash";
import { ICustomer } from "./common/types/Customer";
import {
  DB_CUSTOMERS_COLLECTION_NAME,
  DB_CUSTOMERS_ANONYMISED_COLLECTION_NAME,
  DB_NAME,
  SYNC_BATCH_FLUSH_TIMEOUT_MS,
  SUNC_BATCH_FLUSH_SIZE,
} from "./common/constraints";
import { config } from "./common/config";

class SyncApp {
  client: MongoClient = new MongoClient(config.dbUrl);
  changeStream: ChangeStream<
    ICustomer,
    | ChangeStreamInsertDocument<ICustomer>
    | ChangeStreamUpdateDocument<ICustomer>
  >;
  db: Db;
  inputCollection: Collection<ICustomer>;
  outputCollection: Collection<ICustomer>;

  batch: ICustomer[] = [];

  async init() {
    await this.client.connect();

    this.db = this.client.db(DB_NAME);

    this.inputCollection = this.db.collection<ICustomer>(
      DB_CUSTOMERS_COLLECTION_NAME
    );
    this.outputCollection = this.db.collection<ICustomer>(
      DB_CUSTOMERS_ANONYMISED_COLLECTION_NAME
    );
  }

  async start() {
    if (argv.includes("--full-reindex")) {
      await this.fullReindex();
    } else {
      this.subscribe();
      await this.retrieveLastUpdates();
    }
  }

  async shutdown(success: boolean = true) {
    console.log("exiting...");
    try {
      await this.changeStream?.close();
      await this.client?.close();
    } catch (err) {
      success = false;
      console.log("error during shutdown");
    } finally {
      process.exit(success ? 0 : 1);
    }
  }

  onError(error: Error) {
    console.log(error);
    this.shutdown(false);
  }

  private async fullReindex() {
    if ((await this.outputCollection.countDocuments({})) > 0)
      await this.outputCollection.drop();
    this.outputCollection = this.db.collection<ICustomer>(
      DB_CUSTOMERS_ANONYMISED_COLLECTION_NAME
    );
    var cursor = this.inputCollection.find({});

    var data = [];
    while (await cursor.hasNext()) {
      const customer = (await cursor.next()) as ICustomer;
      data.push(this.anonymize(customer));

      if (data.length > 100) {
        let promises = data.map((d) =>
          this.outputCollection.findOneAndReplace({ _id: d._id }, d, {
            upsert: true,
          })
        );
        await Promise.all(promises);
        data = [];
      }
    }

    let promises = data.map((d) =>
      this.outputCollection.findOneAndReplace({ _id: d._id }, d, {
        upsert: true,
      })
    );
    await Promise.all(promises);

    console.log("success");
    this.shutdown(true);
  }

  private subscribe() {
    this.changeStream = this.inputCollection.watch(
      [{ $match: { operationType: { $in: ["insert", "update"] } } }],
      { fullDocument: "updateLookup" }
    );

    this.changeStream.on("change", (change) => {
      this.batch.push(this.anonymize(change.fullDocument as ICustomer));

      if (this.batch.length === SUNC_BATCH_FLUSH_SIZE) {
        console.log("flush batch by size");
        this.processBatch();
      }
    });

    setTimeout(this.subscribe_timer_cb.bind(this), SYNC_BATCH_FLUSH_TIMEOUT_MS);
  }

  private subscribe_timer_cb() {
    console.log("flush batch by timer");
    this.processBatch();
    setTimeout(this.subscribe_timer_cb.bind(this), SYNC_BATCH_FLUSH_TIMEOUT_MS);
  }

  private async processBatch() {
    if (this.batch.length == 0) return;
    const toProcess = [...this.batch];
    this.batch = [];

    const promises: Promise<any>[] = [];

    for (let doc of toProcess) {
      promises.push(
        this.outputCollection.findOneAndReplace({ _id: doc._id }, doc, {
          upsert: true,
        })
      );
    }
    await Promise.all(promises);
    console.log(`proccessed batch, size ${toProcess.length}`);
  }

  private async retrieveLastUpdates() {
    const lastAdded = await this.outputCollection.findOne(
      {},
      { sort: { createdAt: -1 } }
    );
    const opts: Filter<ICustomer> = {};
    if (lastAdded) opts.createdAt = { $gt: lastAdded?.createdAt };
    const toAdd = await this.inputCollection.find(opts).toArray();
    //in order to check *updated* documents we should add "updatedAt" column to the model.

    if (toAdd.length > 0)
      await this.outputCollection.insertMany(
        toAdd.map(this.anonymize.bind(this))
      );
  }

  private anonymize(customer: ICustomer) {
    let output: ICustomer = cloneDeep(customer);

    output.firstName = this.getRandomString(customer.firstName);
    output.lastName = this.getRandomString(customer.firstName);

    const [localPart, domain] = customer.email.split("@");
    output.email = `${this.getRandomString(localPart)}@${domain}`;

    output.address.line1 = this.getRandomString(customer.firstName);
    output.address.line2 = this.getRandomString(customer.firstName);
    output.address.postcode = this.getRandomString(customer.address.postcode);

    return output;
  }

  private getRandomString(str: string) {
    var s = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    var rng = seedrandom(str);
    return Array.from({ length: 8 }, () =>
      s.charAt(Math.floor(rng() * s.length))
    ).join("");
  }
}

const app = new SyncApp();
const onError = app.onError.bind(app);

process.on("uncaughtException", onError);
process.on("unhandledRejection", onError);
process.on("SIGINT", () => app.shutdown());

async function main() {
  await app.init();
  app.start();
}

main().catch(onError);
