//more complex validation could be here if project would be bigger
if (!process.env.DB_URI) throw new Error("you have to specify DB_URI");

export const config = {
  dbUrl: process.env.DB_URI,
};
