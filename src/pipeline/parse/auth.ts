import { decodeBase64 } from "../../deps.ts";
import { unauthorized } from "../../shared/errors.ts";
import { HEADER } from "../../shared/constants.ts";

interface AuthInfo {
  dbUser: string;
  dbPassword: string;
}

export function parseAuth(h: Headers): AuthInfo {
  const auth = h.get(HEADER.AUTHORIZATION);
  if (!auth) throw unauthorized("missing_authorization_header");
  const [scheme, token] = auth.split(/\s+/, 2);
  if (!scheme || !token) throw unauthorized("malformed_authorization_header");

  if (/^basic$/i.test(scheme)) {
    try {
      const decoded = new TextDecoder().decode(decodeBase64(token));
      const idx = decoded.indexOf(":");
      if (idx <= 0) throw unauthorized("invalid_basic_credentials");
      const user = decoded.slice(0, idx);
      const pass = decoded.slice(idx + 1);
      if (!user) throw unauthorized("missing_basic_username");
      return { dbUser: user, dbPassword: pass };
    } catch {
      throw unauthorized("invalid_basic_token");
    }
  }

  if (/^bearer$/i.test(scheme)) {
    const dbUser = h.get(HEADER.DB_USER) ?? "";
    if (!dbUser) throw unauthorized("missing_x_db_user_for_bearer");
    return { dbUser, dbPassword: token };
  }

  throw unauthorized("unsupported_auth_scheme");
}
