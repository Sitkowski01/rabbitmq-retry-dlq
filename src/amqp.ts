import amqp, { type ChannelModel } from "amqplib";

export const URL = process.env.AMQP_URL ?? "amqp://guest:guest@localhost:5672";

const ts = () => new Date().toISOString().slice(11, 19);
export const log = (icon: string, msg: string) => console.log(`${ts()} ${icon} ${msg}`);

/**
 * Broker w Dockerze potrzebuje kilkunastu sekund na start, a `docker compose up -d`
 * wraca natychmiast. Bez tego pierwsze uruchomienie po `up` kończyło się ECONNREFUSED.
 */
export async function connectWithRetry(
  proby = 15,
  odstepMs = 2000,
): Promise<ChannelModel> {
  for (let i = 1; i <= proby; i++) {
    try {
      return await amqp.connect(URL);
    } catch (e) {
      if (i === proby) throw e;
      log("⏳", `broker jeszcze nie odpowiada (${i}/${proby}) — ponawiam za ${odstepMs / 1000}s`);
      await new Promise((r) => setTimeout(r, odstepMs));
    }
  }
  throw new Error("nieosiągalne");
}

/**
 * Bez tych nasłuchów restart brokera podnosi `Unhandled 'error' event`
 * i ubija proces — potok zatrzymywał się po cichu.
 */
export function pilnujPolaczenia(conn: ChannelModel): void {
  conn.on("error", (e) => log("⚠️ ", `błąd połączenia: ${e.message}`));
  conn.on("close", () => {
    log("🔌", "połączenie z brokerem zamknięte");
    process.exitCode = 1;
  });
}
