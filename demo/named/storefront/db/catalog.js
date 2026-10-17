// Copyright (c) 2026 Kenneth Stott
// Canary: 79c4e989-0ef9-4126-aa79-63a5412ee94a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.
//
// The storefront's product catalog, one MongoDB document a product (db/mongo-reader.js makes the
// reader Provisa reads it as). A product's price is the unit price an order line of it records
// (db/init.sql): the eight prices in turn, by its id.

const prices = [12.5, 18.0, 24.0, 32.5, 45.0, 59.0, 79.0, 120.0];
const names = [
  ["Pour-over kettle", "brewing"],
  ["Ceramic dripper", "brewing"],
  ["Paper filters (100)", "brewing"],
  ["Burr grinder", "brewing"],
  ["House blend, 1 kg", "coffee"],
  ["Single origin, Ethiopia", "coffee"],
  ["Single origin, Colombia", "coffee"],
  ["Decaf, 1 kg", "coffee"],
  ["Travel mug", "drinkware"],
  ["Espresso cups (2)", "drinkware"],
  ["Glass carafe", "drinkware"],
  ["Latte bowl", "drinkware"],
  ["Milk frother", "equipment"],
  ["Digital scale", "equipment"],
  ["Cold brew jar", "equipment"],
  ["Tamper", "equipment"],
  ["Gift card, 25", "gifts"],
  ["Gift card, 50", "gifts"],
  ["Tasting set", "gifts"],
  ["Subscription, 3 months", "gifts"],
  ["Tote bag", "merchandise"],
  ["Apron", "merchandise"],
  ["Poster", "merchandise"],
  ["Cap", "merchandise"],
];

// Each product's picture, carried in the catalog itself: a tile in its category's colour with its
// initials, as an SVG data URL, so an app showing it needs nothing else.
const colours = {
  brewing: "#8d6e63",
  coffee: "#5d4037",
  drinkware: "#26a69a",
  equipment: "#5c6bc0",
  gifts: "#ec407a",
  merchandise: "#ffa726",
};
function picture(name, category) {
  const initials = name
    .split(/[ ,]+/)
    .filter((w) => /^[A-Z]/.test(w))
    .slice(0, 2)
    .map((w) => w[0])
    .join("");
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96">` +
    `<rect width="96" height="96" rx="12" fill="${colours[category]}"/>` +
    `<text x="48" y="58" font-family="Helvetica,Arial" font-size="32" fill="#fff" ` +
    `text-anchor="middle">${initials}</text></svg>`;
  return "data:image/svg+xml;utf8," + encodeURIComponent(svg);
}

db = db.getSiblingDB("catalog");
db.products.insertMany(
  names.map(([name, category], i) => ({
    _id: i + 1,
    name: name,
    category: category,
    price: prices[i % 8],
    image_url: picture(name, category),
    details: { origin: category === "coffee" ? name.split(", ").pop() : null, in_stock: (i * 7) % 5 !== 0 },
  })),
);
