import { Document } from 'mongodb'
export interface ICustomer extends Document {
  firstName: string;
  lastName: string;
  email: string;
  address: {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
  },
  createdAt: string
}