const { WebpackManifestPlugin } = require('webpack-manifest-plugin');
const path = require("path");

module.exports = {
    mode: "development",
    entry: {
        clientapp: path.resolve(__dirname, "src/client/rexer_app/index.tsx"),
        signon: path.resolve(__dirname, "src/client/signon/index.tsx"),
    },
    output: {
        path: path.resolve(__dirname, "dist"),
        filename: "[name].[contenthash].js",
        clean: true,
    },
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
                test: /\.less$/i,
                use: [
                    "style-loader",
                    "css-loader",
                    {
                        loader: "less-loader",
                        options: {
                            lessOptions: {
                                javascriptEnabled: true,
                            },
                        },
                    }
                ],
            },
            {
                test: /\.module\.svg$/i,
                use: ['@svgr/webpack'],
            },
        ]
    },
    resolve: {
        extensions: [".tsx", ".ts", ".js", ".jsx", ".scss", ".sass", ".less"]
    },
    plugins: [
        new WebpackManifestPlugin({
            publicPath: "",
        }),
    ]
}
