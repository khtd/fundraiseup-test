import { faker } from "@faker-js/faker";
import { MongoClient } from "mongodb";
import { ICustomer } from "./common/types/Customer";
import {
  DB_CUSTOMERS_COLLECTION_NAME,
  DB_NAME,
  NEW_CUSTOMERS_MAX,
  NEW_CUSTOMERS_MIN,
  GENERATION_INTERVAL_MS,
} from "./common/constraints";
import { config } from "./common/config";

let timer: NodeJS.Timeout;
const client = new MongoClient(config.dbUrl);

function createCustomer() {
  let customer: ICustomer;
  const firstName = faker.person.firstName();
  const lastName = faker.person.lastName();
  customer = {
    firstName,
    lastName,
    email: `${firstName}.${lastName}@hotmail.com`,
    address: {
      line1: faker.location.streetAddress(),
      line2: faker.location.secondaryAddress(),
      postcode: faker.location.zipCode(),
      city: faker.location.city(),
      state: faker.location.state({ abbreviated: true }),
      country: "US",
    },
    createdAt: new Date().toISOString(),
  };

  return customer;
}

function createMultipleCustomers(amount: number): ICustomer[] {
  return Array.from({ length: amount }, () => createCustomer());
}

function getRandomAmount() {
  return (
    Math.floor(Math.random() * (NEW_CUSTOMERS_MAX - NEW_CUSTOMERS_MIN)) +
    NEW_CUSTOMERS_MIN
  );
}

async function execute() {
  const newCustomers = createMultipleCustomers(getRandomAmount());

  const collection = client
    .db(DB_NAME)
    .collection<ICustomer>(DB_CUSTOMERS_COLLECTION_NAME);
  await collection.insertMany(newCustomers);
}

function shutdown(success: boolean = true) {
  console.log("exiting...");

  clearTimeout(timer);
  client.close();

  process.exit(success ? 0 : 1);
}

function onError(error: Error) {
  console.log(error);
  shutdown(false);
}

async function main() {
  await client.connect();
  timer = setInterval(() => execute().catch(onError), GENERATION_INTERVAL_MS);
}

process.on("uncaughtException", onError);
process.on("unhandledRejection", onError);
process.on("SIGINT", () => shutdown());

main().catch(onError);
