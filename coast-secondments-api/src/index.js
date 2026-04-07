const CSV_KEY = "secondments/secondments_description.csv";
const ALLOWED_ORIGIN = "https://defreitasl.github.io";

function corsHeaders(origin) {
  const allowed = origin === ALLOWED_ORIGIN ? origin : ALLOWED_ORIGIN;
  return {
    "Access-Control-Allow-Origin": allowed,
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin",
  };
}

function normalizeInstitution(value) {
  return String(value || "").trim().toLowerCase();
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (text.includes('"') || text.includes(",") || text.includes("\n")) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function cleanHeader(header) {
  return String(header ?? "")
    .replace(/^\uFEFF/, "")
    .trim();
}

function parseCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      out.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  out.push(current);
  return out;
}

function parseCsv(text) {
  const safeText = String(text || "").replace(/^\uFEFF/, "");
  const lines = safeText.split(/\r?\n/).filter(line => line.trim() !== "");
  if (!lines.length) return { headers: [], rows: [] };

  const headers = parseCsvLine(lines[0]).map(cleanHeader);

  const rows = lines.slice(1).map(line => {
    const values = parseCsvLine(line);
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = values[i] ?? "";
    });
    return obj;
  });

  return { headers, rows };
}

function buildCsv(headers, rows) {
  const headerLine = headers.map(csvEscape).join(",");
  const rowLines = rows.map(row =>
    headers.map(h => csvEscape(row[h] ?? "")).join(",")
  );
  return [headerLine, ...rowLines].join("\n") + "\n";
}

async function getCurrentCsv(bucket) {
  const obj = await bucket.get(CSV_KEY);
  if (!obj) {
    return "Institution,Name,Project phase,Host institution,M# (start),M# (end),WP,Secondment description\n";
  }
  const text = await obj.text();
  return String(text || "").replace(/^\uFEFF/, "");
}

function upgradeLegacyRows(headers, rows) {
  const hasInstitution = headers.includes("Institution");
  const hasWP = headers.includes("WP");

  const upgradedHeaders = [
    "Institution",
    "Name",
    "Project phase",
    "Host institution",
    "M# (start)",
    "M# (end)",
    "WP",
    "Secondment description",
  ];

  const upgradedRows = rows.map(row => ({
    "Institution": hasInstitution ? (row["Institution"] ?? "") : "",
    "Name": row["Name"] ?? "",
    "Project phase": row["Project phase"] ?? "",
    "Host institution": row["Host institution"] ?? "",
    "M# (start)": row["M# (start)"] ?? "",
    "M# (end)": row["M# (end)"] ?? "",
    "WP": hasWP ? (row["WP"] ?? "") : "",
    "Secondment description": row["Secondment description"] ?? "",
  }));

  return {
    headers: upgradedHeaders,
    rows: upgradedRows,
  };
}

function validatePayload(body) {
  if (!body || typeof body !== "object") {
    throw new Error("Invalid payload.");
  }

  if (!body.institution || !String(body.institution).trim()) {
    throw new Error("Institution is required.");
  }

  if (!Array.isArray(body.participants) || body.participants.length === 0) {
    throw new Error("At least one participant is required.");
  }

  for (const p of body.participants) {
    const phaseOk = ["Phase 1", "Phase 2", "Phase 3"].includes(p.project_phase);
    const wpOk = ["WP1", "WP2", "WP3"].includes(p.wp);
    const mStart = Number(p.m_start);
    const mEnd = Number(p.m_end);

    if (!p.name || !String(p.name).trim()) throw new Error("Participant name is required.");
    if (!phaseOk) throw new Error("Invalid project phase.");
    if (!p.host_institution || !String(p.host_institution).trim()) throw new Error("Host institution is required.");
    if (!Number.isInteger(mStart) || mStart < 4 || mStart > 47) throw new Error("M# start must be 4–47.");
    if (!Number.isInteger(mEnd) || mEnd < 5 || mEnd > 48) throw new Error("M# end must be 5–48.");
    if (mEnd <= mStart) throw new Error("M# end must be greater than M# start.");
    if (!wpOk) throw new Error("WP must be WP1, WP2 or WP3.");
    if (!p.secondment_description || !String(p.secondment_description).trim()) {
      throw new Error("Secondment description is required.");
    }
  }
}

async function verifyTurnstile(token, env, remoteIp) {
  const formData = new FormData();
  formData.append("secret", env.TURNSTILE_SECRET);
  formData.append("response", token);
  if (remoteIp) formData.append("remoteip", remoteIp);

  const resp = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
    method: "POST",
    body: formData,
  });

  return await resp.json();
}

export default {
  async fetch(request, env) {
    const origin = request.headers.get("Origin") || "";
    const headers = corsHeaders(origin);
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    if (url.pathname === "/api/secondments.csv" && request.method === "GET") {
      const csv = await getCurrentCsv(env.SECONDMENTS_BUCKET);
      return new Response(csv, {
        status: 200,
        headers: {
          ...headers,
          "Content-Type": "text/csv; charset=utf-8",
          "Cache-Control": "no-store",
        },
      });
    }

    if (url.pathname === "/api/secondments" && request.method === "GET") {
      const institution = url.searchParams.get("institution") || "";
      const csv = await getCurrentCsv(env.SECONDMENTS_BUCKET);
      const parsed = parseCsv(csv);
      const upgraded = upgradeLegacyRows(parsed.headers, parsed.rows);
      const target = normalizeInstitution(institution);

      const matches = upgraded.rows
        .filter(row => normalizeInstitution(row["Institution"]) === target)
        .map(row => ({
          name: row["Name"] || "",
          project_phase: row["Project phase"] || "",
          host_institution: row["Host institution"] || "",
          m_start: row["M# (start)"] || "",
          m_end: row["M# (end)"] || "",
          wp: row["WP"] || "",
          secondment_description: row["Secondment description"] || "",
        }));

      return Response.json(
        {
          ok: true,
          institution,
          participants: matches,
        },
        { status: 200, headers }
      );
    }

    if (url.pathname === "/api/secondments" && request.method === "POST") {
      try {
        const body = await request.json();
        validatePayload(body);

        const turnstileToken = body.turnstileToken;
        if (!turnstileToken) {
          return Response.json({ ok: false, error: "Missing Turnstile token." }, { status: 400, headers });
        }

        const remoteIp = request.headers.get("CF-Connecting-IP") || undefined;
        const verification = await verifyTurnstile(turnstileToken, env, remoteIp);

        if (!verification.success) {
          return Response.json(
            { ok: false, error: "Turnstile verification failed.", details: verification["error-codes"] || [] },
            { status: 403, headers }
          );
        }

        const institution = String(body.institution).trim();
        const target = normalizeInstitution(institution);

        const currentCsv = await getCurrentCsv(env.SECONDMENTS_BUCKET);
        const parsed = parseCsv(currentCsv);
        const upgraded = upgradeLegacyRows(parsed.headers, parsed.rows);

        const keptRows = upgraded.rows.filter(
          row => normalizeInstitution(row["Institution"]) !== target
        );

        const newRows = body.participants.map(p => ({
          "Institution": institution,
          "Name": String(p.name).trim(),
          "Project phase": p.project_phase,
          "Host institution": String(p.host_institution).trim(),
          "M# (start)": String(Number(p.m_start)),
          "M# (end)": String(Number(p.m_end)),
          "WP": p.wp,
          "Secondment description": String(p.secondment_description).trim(),
        }));

        const finalRows = [...keptRows, ...newRows];
        const nextCsv = buildCsv(
          [
            "Institution",
            "Name",
            "Project phase",
            "Host institution",
            "M# (start)",
            "M# (end)",
            "WP",
            "Secondment description",
          ],
          finalRows
        );

        await env.SECONDMENTS_BUCKET.put(CSV_KEY, nextCsv, {
          httpMetadata: {
            contentType: "text/csv; charset=utf-8",
          },
        });

        return Response.json(
          {
            ok: true,
            institution,
            replaced_with_rows: newRows.length,
          },
          { status: 200, headers }
        );
      } catch (err) {
        return Response.json(
          { ok: false, error: err instanceof Error ? err.message : "Unknown error." },
          { status: 400, headers }
        );
      }
    }

    return new Response("Not found", { status: 404, headers });
  },
};