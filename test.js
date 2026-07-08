const express = require('express');
const app = express();

app.get('/ping', (req, res) => {
  console.log("🔍 /ping route hit");
  res.json({ message: "pong" });
});

app.listen(3000, '0.0.0.0', () => {
  console.log("🚀 Test server running on http://localhost:3000");
});
