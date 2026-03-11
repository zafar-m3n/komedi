db = db.getSiblingDB("jokes_db");

db.types.deleteMany({});
db.jokes.deleteMany({});

db.types.insertMany([{ name: "Dad" }, { name: "Knock Knock" }, { name: "Programming" }]);

db.jokes.insertMany([
  {
    setup: "Why don’t skeletons fight each other?",
    punchline: "They don’t have the guts.",
    type: "Dad",
  },
  {
    setup: "Why did the scarecrow win an award?",
    punchline: "Because he was outstanding in his field.",
    type: "Dad",
  },
  {
    setup: "Why can’t you give Elsa a balloon?",
    punchline: "Because she will let it go.",
    type: "Dad",
  },
  {
    setup: "Why did the bicycle fall over?",
    punchline: "Because it was two tired.",
    type: "Dad",
  },
  {
    setup: "What do you call fake spaghetti?",
    punchline: "An impasta.",
    type: "Dad",
  },
  {
    setup: "Knock knock. Who’s there? Lettuce.",
    punchline: "Lettuce who? Lettuce in, it’s cold out here!",
    type: "Knock Knock",
  },
  {
    setup: "Knock knock. Who’s there? Cow says.",
    punchline: "Cow says who? No silly, cow says moo!",
    type: "Knock Knock",
  },
  {
    setup: "Knock knock. Who’s there? Tank.",
    punchline: "Tank who? You’re welcome.",
    type: "Knock Knock",
  },
  {
    setup: "Knock knock. Who’s there? Boo.",
    punchline: "Boo who? Don’t cry, it’s just a joke.",
    type: "Knock Knock",
  },
  {
    setup: "Knock knock. Who’s there? Olive.",
    punchline: "Olive who? Olive you and I miss you.",
    type: "Knock Knock",
  },
  {
    setup: "Why do programmers prefer dark mode?",
    punchline: "Because light attracts bugs.",
    type: "Programming",
  },
  {
    setup: "Why do Java developers wear glasses?",
    punchline: "Because they don’t see sharp.",
    type: "Programming",
  },
  {
    setup: "Why did the programmer quit his job?",
    punchline: "Because he didn’t get arrays.",
    type: "Programming",
  },
  {
    setup: "How many programmers does it take to change a light bulb?",
    punchline: "None. It’s a hardware problem.",
    type: "Programming",
  },
  {
    setup: "Why was the JavaScript developer sad?",
    punchline: "Because he didn’t Node how to Express himself.",
    type: "Programming",
  },
]);
