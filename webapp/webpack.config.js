const path = require("path");

module.exports = {
    mode: "development",
    entry: {
        clientapp: path.resolve(__dirname, "src/client/index.tsx"),
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
                test: /\.tsx?$/,
                use: "ts-loader",
                exclude: /node_modules/,
            },
            {
                test: /\.s[ac]ss$/i,
                use: [
                  "style-loader",
                  "css-modules-typescript-loader",
                  {
                    loader: "css-loader",
                    options: {
                        modules: true,
                    },
                  },
                  "sass-loader",
                ],
            },
            {
                test: /\.css/i,
                use: [
                    "style-loader",
                    "css-loader",
                ],
            },
        ]
    },
    resolve: {
        extensions: [".tsx", ".ts", ".js", ".jsx", ".css", ".scss", ".sass"]
    }
}
