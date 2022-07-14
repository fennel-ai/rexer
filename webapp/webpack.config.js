const path = require("path");

module.exports = {
    mode: "development",
    entry: {
        clientapp: path.resolve(__dirname, "src/client/index.js"),
    },
    output: {
        path: path.resolve(__dirname, "dist"),
        filename: "[name].js",
        clean: true,
    },
    devtool: "source-map",
    module: {
        rules: [
            {
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                use: ["babel-loader"],
            }
        ],
    }
}
