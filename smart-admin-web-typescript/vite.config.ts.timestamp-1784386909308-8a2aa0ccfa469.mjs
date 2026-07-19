// vite.config.ts
import { resolve } from "path";
import vue from "file:///D:/01workspace/smart-admin/smart-admin-web-typescript/node_modules/@vitejs/plugin-vue/dist/index.mjs";
import { loadEnv } from "file:///D:/01workspace/smart-admin/smart-admin-web-typescript/node_modules/vite/dist/node/index.js";

// src/theme/custom-variables.ts
import { theme } from "file:///D:/01workspace/smart-admin/smart-admin-web-typescript/node_modules/ant-design-vue/lib/index.js";
import convertLegacyToken from "file:///D:/01workspace/smart-admin/smart-admin-web-typescript/node_modules/ant-design-vue/lib/theme/convertLegacyToken.js";
var { defaultAlgorithm, defaultSeed } = theme;
var mapToken = defaultAlgorithm(defaultSeed);
var token = convertLegacyToken.default(mapToken);
var custom_variables_default = {
  "@primary-color": token["primary-color"],
  // 全局主色
  "@base-bg-color": "#fff",
  "@hover-bg-color": "rgba(0, 0, 0, 0.025)",
  "@hover-bg-color-night": "rgba(255, 255, 255, 0.025)",
  "@header-light-bg-hover-color": "#f6f6f6",
  "@header-height": "80px",
  "@header-user-height": "40px",
  "@page-tag-height": "40px",
  "@theme-list": ["light", "dark", "night"]
};

// vite.config.ts
var __vite_injected_original_dirname = "D:\\01workspace\\smart-admin\\smart-admin-web-typescript";
var pathResolve = (dir) => {
  return resolve(__vite_injected_original_dirname, ".", dir);
};
var vite_config_default = ({ mode }) => {
  const env = loadEnv(mode, process.cwd());
  return {
    base: process.env.NODE_ENV === "production" ? "/" : "/",
    root: process.cwd(),
    resolve: {
      alias: [
        // 国际化替换
        {
          find: "vue-i18n",
          replacement: "vue-i18n/dist/vue-i18n.cjs.js"
        },
        // 绝对路径重命名：/@/xxxx => src/xxxx
        {
          find: /\/@\//,
          replacement: pathResolve("src") + "/"
        },
        {
          find: /^~/,
          replacement: ""
        }
      ]
    },
    server: {
      host: "0.0.0.0",
      port: 8081,
      server: {
        proxy: {
          // 代理路径
          "/": {
            target: env.VITE_APP_API_URL,
            // 目标服务器地址
            changeOrigin: true,
            // 是否修改请求头中的 Origin 字段
            rewrite: (path) => path
            // 重写路径
          }
        }
      }
    },
    plugins: [vue()],
    optimizeDeps: {
      include: ["ant-design-vue/es/locale/zh_CN", "dayjs/locale/zh-cn", "ant-design-vue/es/locale/en_US"],
      exclude: ["vue-demi"]
    },
    build: {
      // 清除console和debugger
      terserOptions: {
        compress: {
          drop_console: true,
          drop_debugger: true
        }
      },
      rollupOptions: {
        output: {
          //配置这个是让不同类型文件放在不同文件夹，不会显得太乱
          chunkFileNames: "js/[name]-[hash].js",
          entryFileNames: "js/[name]-[hash].js",
          assetFileNames: "[ext]/[name]-[hash].[ext]",
          manualChunks(id) {
            if (id.includes("node_modules")) {
              return id.toString().split("node_modules/")[1].split("/")[0].toString();
            }
          }
        }
      },
      target: "esnext",
      outDir: "dist",
      // 指定输出路径
      assetsDir: "assets",
      // 指定生成静态文件目录
      assetsInlineLimit: "4096",
      // 小于此阈值的导入或引用资源将内联为 base64 编码
      chunkSizeWarningLimit: 500,
      // chunk 大小警告的限制
      minify: "terser",
      // 混淆器，terser构建后文件体积更小
      emptyOutDir: true
      //打包前先清空原有打包文件
    },
    css: {
      preprocessorOptions: {
        less: {
          modifyVars: custom_variables_default,
          javascriptEnabled: true
        }
      }
    },
    define: {
      __INTLIFY_PROD_DEVTOOLS__: false,
      "process.env": process.env
    }
  };
};
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsidml0ZS5jb25maWcudHMiLCAic3JjL3RoZW1lL2N1c3RvbS12YXJpYWJsZXMudHMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCJEOlxcXFwwMXdvcmtzcGFjZVxcXFxzbWFydC1hZG1pblxcXFxzbWFydC1hZG1pbi13ZWItdHlwZXNjcmlwdFwiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9maWxlbmFtZSA9IFwiRDpcXFxcMDF3b3Jrc3BhY2VcXFxcc21hcnQtYWRtaW5cXFxcc21hcnQtYWRtaW4td2ViLXR5cGVzY3JpcHRcXFxcdml0ZS5jb25maWcudHNcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfaW1wb3J0X21ldGFfdXJsID0gXCJmaWxlOi8vL0Q6LzAxd29ya3NwYWNlL3NtYXJ0LWFkbWluL3NtYXJ0LWFkbWluLXdlYi10eXBlc2NyaXB0L3ZpdGUuY29uZmlnLnRzXCI7LypcclxuICogdml0ZVx1OTE0RFx1N0Y2RVxyXG4gKlxyXG4gKiBAQXV0aG9yOiAgICAxMDI0XHU1MjFCXHU2NUIwXHU1QjlFXHU5QThDXHU1QkE0LVx1NEUzQlx1NEVGQlx1RkYxQVx1NTM1M1x1NTkyN1xyXG4gKiBARGF0ZTogICAgICAyMDIyLTA1LTAyIDIzOjQ0OjU2XHJcbiAqIEBXZWNoYXQ6ICAgIHpodWRhMTAyNFxyXG4gKiBARW1haWw6ICAgICBsYWIxMDI0QDE2My5jb21cclxuICogQENvcHlyaWdodCAgMTAyNFx1NTIxQlx1NjVCMFx1NUI5RVx1OUE4Q1x1NUJBNCBcdUZGMDggaHR0cHM6Ly8xMDI0bGFiLm5ldCBcdUZGMDlcdUZGMENTaW5jZSAyMDEyXHJcbiAqL1xyXG5pbXBvcnQgeyByZXNvbHZlIH0gZnJvbSAncGF0aCc7XHJcbmltcG9ydCB2dWUgZnJvbSAnQHZpdGVqcy9wbHVnaW4tdnVlJztcclxuaW1wb3J0IHsgbG9hZEVudiB9IGZyb20gJ3ZpdGUnO1xyXG5pbXBvcnQgY3VzdG9tVmFyaWFibGVzIGZyb20gJy9AL3RoZW1lL2N1c3RvbS12YXJpYWJsZXMuanMnO1xyXG5cclxuY29uc3QgcGF0aFJlc29sdmUgPSAoZGlyKSA9PiB7XHJcbiAgcmV0dXJuIHJlc29sdmUoX19kaXJuYW1lLCAnLicsIGRpcik7XHJcbn07XHJcblxyXG5leHBvcnQgZGVmYXVsdCAoeyBtb2RlIH0pID0+IHtcclxuICBjb25zdCBlbnYgPSBsb2FkRW52KG1vZGUsIHByb2Nlc3MuY3dkKCkpO1xyXG4gIHJldHVybiB7XHJcbiAgICBiYXNlOiBwcm9jZXNzLmVudi5OT0RFX0VOViA9PT0gJ3Byb2R1Y3Rpb24nID8gJy8nIDogJy8nLFxyXG4gICAgcm9vdDogcHJvY2Vzcy5jd2QoKSxcclxuICAgIHJlc29sdmU6IHtcclxuICAgICAgYWxpYXM6IFtcclxuICAgICAgICAvLyBcdTU2RkRcdTk2NDVcdTUzMTZcdTY2RkZcdTYzNjJcclxuICAgICAgICB7XHJcbiAgICAgICAgICBmaW5kOiAndnVlLWkxOG4nLFxyXG4gICAgICAgICAgcmVwbGFjZW1lbnQ6ICd2dWUtaTE4bi9kaXN0L3Z1ZS1pMThuLmNqcy5qcycsXHJcbiAgICAgICAgfSxcclxuICAgICAgICAvLyBcdTdFRERcdTVCRjlcdThERUZcdTVGODRcdTkxQ0RcdTU0N0RcdTU0MERcdUZGMUEvQC94eHh4ID0+IHNyYy94eHh4XHJcbiAgICAgICAge1xyXG4gICAgICAgICAgZmluZDogL1xcL0BcXC8vLFxyXG4gICAgICAgICAgcmVwbGFjZW1lbnQ6IHBhdGhSZXNvbHZlKCdzcmMnKSArICcvJyxcclxuICAgICAgICB9LFxyXG4gICAgICAgIHtcclxuICAgICAgICAgIGZpbmQ6IC9efi8sXHJcbiAgICAgICAgICByZXBsYWNlbWVudDogJycsXHJcbiAgICAgICAgfSxcclxuICAgICAgXSxcclxuICAgIH0sXHJcbiAgICBzZXJ2ZXI6IHtcclxuICAgICAgaG9zdDogJzAuMC4wLjAnLFxyXG4gICAgICBwb3J0OiA4MDgxLFxyXG4gICAgICBzZXJ2ZXI6IHtcclxuICAgICAgICBwcm94eToge1xyXG4gICAgICAgICAgLy8gXHU0RUUzXHU3NDA2XHU4REVGXHU1Rjg0XHJcbiAgICAgICAgICAnLyc6IHtcclxuICAgICAgICAgICAgdGFyZ2V0OiBlbnYuVklURV9BUFBfQVBJX1VSTCwgLy8gXHU3NkVFXHU2ODA3XHU2NzBEXHU1MkExXHU1NjY4XHU1NzMwXHU1NzQwXHJcbiAgICAgICAgICAgIGNoYW5nZU9yaWdpbjogdHJ1ZSwgLy8gXHU2NjJGXHU1NDI2XHU0RkVFXHU2NTM5XHU4QkY3XHU2QzQyXHU1OTM0XHU0RTJEXHU3Njg0IE9yaWdpbiBcdTVCNTdcdTZCQjVcclxuICAgICAgICAgICAgcmV3cml0ZTogKHBhdGgpID0+IHBhdGgsIC8vIFx1OTFDRFx1NTE5OVx1OERFRlx1NUY4NFxyXG4gICAgICAgICAgfSxcclxuICAgICAgICB9LFxyXG4gICAgICB9LFxyXG4gICAgfSxcclxuICAgIHBsdWdpbnM6IFt2dWUoKV0sXHJcbiAgICBvcHRpbWl6ZURlcHM6IHtcclxuICAgICAgaW5jbHVkZTogWydhbnQtZGVzaWduLXZ1ZS9lcy9sb2NhbGUvemhfQ04nLCAnZGF5anMvbG9jYWxlL3poLWNuJywgJ2FudC1kZXNpZ24tdnVlL2VzL2xvY2FsZS9lbl9VUyddLFxyXG4gICAgICBleGNsdWRlOiBbJ3Z1ZS1kZW1pJ10sXHJcbiAgICB9LFxyXG4gICAgYnVpbGQ6IHtcclxuICAgICAgLy8gXHU2RTA1XHU5NjY0Y29uc29sZVx1NTQ4Q2RlYnVnZ2VyXHJcbiAgICAgIHRlcnNlck9wdGlvbnM6IHtcclxuICAgICAgICBjb21wcmVzczoge1xyXG4gICAgICAgICAgZHJvcF9jb25zb2xlOiB0cnVlLFxyXG4gICAgICAgICAgZHJvcF9kZWJ1Z2dlcjogdHJ1ZSxcclxuICAgICAgICB9LFxyXG4gICAgICB9LFxyXG4gICAgICByb2xsdXBPcHRpb25zOiB7XHJcbiAgICAgICAgb3V0cHV0OiB7XHJcbiAgICAgICAgICAvL1x1OTE0RFx1N0Y2RVx1OEZEOVx1NEUyQVx1NjYyRlx1OEJBOVx1NEUwRFx1NTQwQ1x1N0M3Qlx1NTc4Qlx1NjU4N1x1NEVGNlx1NjUzRVx1NTcyOFx1NEUwRFx1NTQwQ1x1NjU4N1x1NEVGNlx1NTkzOVx1RkYwQ1x1NEUwRFx1NEYxQVx1NjYzRVx1NUY5N1x1NTkyQVx1NEU3MVxyXG4gICAgICAgICAgY2h1bmtGaWxlTmFtZXM6ICdqcy9bbmFtZV0tW2hhc2hdLmpzJyxcclxuICAgICAgICAgIGVudHJ5RmlsZU5hbWVzOiAnanMvW25hbWVdLVtoYXNoXS5qcycsXHJcbiAgICAgICAgICBhc3NldEZpbGVOYW1lczogJ1tleHRdL1tuYW1lXS1baGFzaF0uW2V4dF0nLFxyXG4gICAgICAgICAgbWFudWFsQ2h1bmtzKGlkKSB7XHJcbiAgICAgICAgICAgIC8vXHU5NzU5XHU2MDAxXHU4RDQ0XHU2RTkwXHU1MjA2XHU2MkM2XHU2MjUzXHU1MzA1XHJcbiAgICAgICAgICAgIGlmIChpZC5pbmNsdWRlcygnbm9kZV9tb2R1bGVzJykpIHtcclxuICAgICAgICAgICAgICByZXR1cm4gaWQudG9TdHJpbmcoKS5zcGxpdCgnbm9kZV9tb2R1bGVzLycpWzFdLnNwbGl0KCcvJylbMF0udG9TdHJpbmcoKTtcclxuICAgICAgICAgICAgfVxyXG4gICAgICAgICAgfSxcclxuICAgICAgICB9LFxyXG4gICAgICB9LFxyXG4gICAgICB0YXJnZXQ6ICdlc25leHQnLFxyXG4gICAgICBvdXREaXI6ICdkaXN0JywgLy8gXHU2MzA3XHU1QjlBXHU4RjkzXHU1MUZBXHU4REVGXHU1Rjg0XHJcbiAgICAgIGFzc2V0c0RpcjogJ2Fzc2V0cycsIC8vIFx1NjMwN1x1NUI5QVx1NzUxRlx1NjIxMFx1OTc1OVx1NjAwMVx1NjU4N1x1NEVGNlx1NzZFRVx1NUY1NVxyXG4gICAgICBhc3NldHNJbmxpbmVMaW1pdDogJzQwOTYnLCAvLyBcdTVDMEZcdTRFOEVcdTZCNjRcdTk2MDhcdTUwM0NcdTc2ODRcdTVCRkNcdTUxNjVcdTYyMTZcdTVGMTVcdTc1MjhcdThENDRcdTZFOTBcdTVDMDZcdTUxODVcdTgwNTRcdTRFM0EgYmFzZTY0IFx1N0YxNlx1NzgwMVxyXG4gICAgICBjaHVua1NpemVXYXJuaW5nTGltaXQ6IDUwMCwgLy8gY2h1bmsgXHU1OTI3XHU1QzBGXHU4QjY2XHU1NDRBXHU3Njg0XHU5NjUwXHU1MjM2XHJcbiAgICAgIG1pbmlmeTogJ3RlcnNlcicsIC8vIFx1NkRGN1x1NkRDNlx1NTY2OFx1RkYwQ3RlcnNlclx1Njc4NFx1NUVGQVx1NTQwRVx1NjU4N1x1NEVGNlx1NEY1M1x1NzlFRlx1NjZGNFx1NUMwRlxyXG4gICAgICBlbXB0eU91dERpcjogdHJ1ZSwgLy9cdTYyNTNcdTUzMDVcdTUyNERcdTUxNDhcdTZFMDVcdTdBN0FcdTUzOUZcdTY3MDlcdTYyNTNcdTUzMDVcdTY1ODdcdTRFRjZcclxuICAgIH0sXHJcbiAgICBjc3M6IHtcclxuICAgICAgcHJlcHJvY2Vzc29yT3B0aW9uczoge1xyXG4gICAgICAgIGxlc3M6IHtcclxuICAgICAgICAgIG1vZGlmeVZhcnM6IGN1c3RvbVZhcmlhYmxlcyxcclxuICAgICAgICAgIGphdmFzY3JpcHRFbmFibGVkOiB0cnVlLFxyXG4gICAgICAgIH0sXHJcbiAgICAgIH0sXHJcbiAgICB9LFxyXG4gICAgZGVmaW5lOiB7XHJcbiAgICAgIF9fSU5UTElGWV9QUk9EX0RFVlRPT0xTX186IGZhbHNlLFxyXG4gICAgICAncHJvY2Vzcy5lbnYnOiBwcm9jZXNzLmVudixcclxuICAgIH0sXHJcbiAgfTtcclxufTtcclxuIiwgImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCJEOlxcXFwwMXdvcmtzcGFjZVxcXFxzbWFydC1hZG1pblxcXFxzbWFydC1hZG1pbi13ZWItdHlwZXNjcmlwdFxcXFxzcmNcXFxcdGhlbWVcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIkQ6XFxcXDAxd29ya3NwYWNlXFxcXHNtYXJ0LWFkbWluXFxcXHNtYXJ0LWFkbWluLXdlYi10eXBlc2NyaXB0XFxcXHNyY1xcXFx0aGVtZVxcXFxjdXN0b20tdmFyaWFibGVzLnRzXCI7Y29uc3QgX192aXRlX2luamVjdGVkX29yaWdpbmFsX2ltcG9ydF9tZXRhX3VybCA9IFwiZmlsZTovLy9EOi8wMXdvcmtzcGFjZS9zbWFydC1hZG1pbi9zbWFydC1hZG1pbi13ZWItdHlwZXNjcmlwdC9zcmMvdGhlbWUvY3VzdG9tLXZhcmlhYmxlcy50c1wiO2ltcG9ydCB7IHRoZW1lIH0gZnJvbSAnYW50LWRlc2lnbi12dWUvbGliJztcclxuaW1wb3J0IGNvbnZlcnRMZWdhY3lUb2tlbiBmcm9tICdhbnQtZGVzaWduLXZ1ZS9saWIvdGhlbWUvY29udmVydExlZ2FjeVRva2VuJztcclxuXHJcbmNvbnN0IHsgZGVmYXVsdEFsZ29yaXRobSwgZGVmYXVsdFNlZWQgfSA9IHRoZW1lO1xyXG5cclxuY29uc3QgbWFwVG9rZW4gPSBkZWZhdWx0QWxnb3JpdGhtKGRlZmF1bHRTZWVkKTtcclxuY29uc3QgdG9rZW4gPSBjb252ZXJ0TGVnYWN5VG9rZW4uZGVmYXVsdChtYXBUb2tlbik7XHJcblxyXG5leHBvcnQgZGVmYXVsdCB7XHJcbiAgJ0BwcmltYXJ5LWNvbG9yJzogdG9rZW5bJ3ByaW1hcnktY29sb3InXSwgLy8gXHU1MTY4XHU1QzQwXHU0RTNCXHU4MjcyXHJcbiAgJ0BiYXNlLWJnLWNvbG9yJzogJyNmZmYnLFxyXG4gICdAaG92ZXItYmctY29sb3InOiAncmdiYSgwLCAwLCAwLCAwLjAyNSknLFxyXG4gICdAaG92ZXItYmctY29sb3ItbmlnaHQnOiAncmdiYSgyNTUsIDI1NSwgMjU1LCAwLjAyNSknLFxyXG4gICdAaGVhZGVyLWxpZ2h0LWJnLWhvdmVyLWNvbG9yJzogJyNmNmY2ZjYnLFxyXG4gICdAaGVhZGVyLWhlaWdodCc6ICc4MHB4JyxcclxuICAnQGhlYWRlci11c2VyLWhlaWdodCc6ICc0MHB4JyxcclxuICAnQHBhZ2UtdGFnLWhlaWdodCc6ICc0MHB4JyxcclxuICAnQHRoZW1lLWxpc3QnOiBbJ2xpZ2h0JywgJ2RhcmsnLCAnbmlnaHQnXSxcclxufTtcclxuIl0sCiAgIm1hcHBpbmdzIjogIjtBQVNBLFNBQVMsZUFBZTtBQUN4QixPQUFPLFNBQVM7QUFDaEIsU0FBUyxlQUFlOzs7QUNYNlcsU0FBUyxhQUFhO0FBQzNaLE9BQU8sd0JBQXdCO0FBRS9CLElBQU0sRUFBRSxrQkFBa0IsWUFBWSxJQUFJO0FBRTFDLElBQU0sV0FBVyxpQkFBaUIsV0FBVztBQUM3QyxJQUFNLFFBQVEsbUJBQW1CLFFBQVEsUUFBUTtBQUVqRCxJQUFPLDJCQUFRO0FBQUEsRUFDYixrQkFBa0IsTUFBTSxlQUFlO0FBQUE7QUFBQSxFQUN2QyxrQkFBa0I7QUFBQSxFQUNsQixtQkFBbUI7QUFBQSxFQUNuQix5QkFBeUI7QUFBQSxFQUN6QixnQ0FBZ0M7QUFBQSxFQUNoQyxrQkFBa0I7QUFBQSxFQUNsQix1QkFBdUI7QUFBQSxFQUN2QixvQkFBb0I7QUFBQSxFQUNwQixlQUFlLENBQUMsU0FBUyxRQUFRLE9BQU87QUFDMUM7OztBRGxCQSxJQUFNLG1DQUFtQztBQWN6QyxJQUFNLGNBQWMsQ0FBQyxRQUFRO0FBQzNCLFNBQU8sUUFBUSxrQ0FBVyxLQUFLLEdBQUc7QUFDcEM7QUFFQSxJQUFPLHNCQUFRLENBQUMsRUFBRSxLQUFLLE1BQU07QUFDM0IsUUFBTSxNQUFNLFFBQVEsTUFBTSxRQUFRLElBQUksQ0FBQztBQUN2QyxTQUFPO0FBQUEsSUFDTCxNQUFNLFFBQVEsSUFBSSxhQUFhLGVBQWUsTUFBTTtBQUFBLElBQ3BELE1BQU0sUUFBUSxJQUFJO0FBQUEsSUFDbEIsU0FBUztBQUFBLE1BQ1AsT0FBTztBQUFBO0FBQUEsUUFFTDtBQUFBLFVBQ0UsTUFBTTtBQUFBLFVBQ04sYUFBYTtBQUFBLFFBQ2Y7QUFBQTtBQUFBLFFBRUE7QUFBQSxVQUNFLE1BQU07QUFBQSxVQUNOLGFBQWEsWUFBWSxLQUFLLElBQUk7QUFBQSxRQUNwQztBQUFBLFFBQ0E7QUFBQSxVQUNFLE1BQU07QUFBQSxVQUNOLGFBQWE7QUFBQSxRQUNmO0FBQUEsTUFDRjtBQUFBLElBQ0Y7QUFBQSxJQUNBLFFBQVE7QUFBQSxNQUNOLE1BQU07QUFBQSxNQUNOLE1BQU07QUFBQSxNQUNOLFFBQVE7QUFBQSxRQUNOLE9BQU87QUFBQTtBQUFBLFVBRUwsS0FBSztBQUFBLFlBQ0gsUUFBUSxJQUFJO0FBQUE7QUFBQSxZQUNaLGNBQWM7QUFBQTtBQUFBLFlBQ2QsU0FBUyxDQUFDLFNBQVM7QUFBQTtBQUFBLFVBQ3JCO0FBQUEsUUFDRjtBQUFBLE1BQ0Y7QUFBQSxJQUNGO0FBQUEsSUFDQSxTQUFTLENBQUMsSUFBSSxDQUFDO0FBQUEsSUFDZixjQUFjO0FBQUEsTUFDWixTQUFTLENBQUMsa0NBQWtDLHNCQUFzQixnQ0FBZ0M7QUFBQSxNQUNsRyxTQUFTLENBQUMsVUFBVTtBQUFBLElBQ3RCO0FBQUEsSUFDQSxPQUFPO0FBQUE7QUFBQSxNQUVMLGVBQWU7QUFBQSxRQUNiLFVBQVU7QUFBQSxVQUNSLGNBQWM7QUFBQSxVQUNkLGVBQWU7QUFBQSxRQUNqQjtBQUFBLE1BQ0Y7QUFBQSxNQUNBLGVBQWU7QUFBQSxRQUNiLFFBQVE7QUFBQTtBQUFBLFVBRU4sZ0JBQWdCO0FBQUEsVUFDaEIsZ0JBQWdCO0FBQUEsVUFDaEIsZ0JBQWdCO0FBQUEsVUFDaEIsYUFBYSxJQUFJO0FBRWYsZ0JBQUksR0FBRyxTQUFTLGNBQWMsR0FBRztBQUMvQixxQkFBTyxHQUFHLFNBQVMsRUFBRSxNQUFNLGVBQWUsRUFBRSxDQUFDLEVBQUUsTUFBTSxHQUFHLEVBQUUsQ0FBQyxFQUFFLFNBQVM7QUFBQSxZQUN4RTtBQUFBLFVBQ0Y7QUFBQSxRQUNGO0FBQUEsTUFDRjtBQUFBLE1BQ0EsUUFBUTtBQUFBLE1BQ1IsUUFBUTtBQUFBO0FBQUEsTUFDUixXQUFXO0FBQUE7QUFBQSxNQUNYLG1CQUFtQjtBQUFBO0FBQUEsTUFDbkIsdUJBQXVCO0FBQUE7QUFBQSxNQUN2QixRQUFRO0FBQUE7QUFBQSxNQUNSLGFBQWE7QUFBQTtBQUFBLElBQ2Y7QUFBQSxJQUNBLEtBQUs7QUFBQSxNQUNILHFCQUFxQjtBQUFBLFFBQ25CLE1BQU07QUFBQSxVQUNKLFlBQVk7QUFBQSxVQUNaLG1CQUFtQjtBQUFBLFFBQ3JCO0FBQUEsTUFDRjtBQUFBLElBQ0Y7QUFBQSxJQUNBLFFBQVE7QUFBQSxNQUNOLDJCQUEyQjtBQUFBLE1BQzNCLGVBQWUsUUFBUTtBQUFBLElBQ3pCO0FBQUEsRUFDRjtBQUNGOyIsCiAgIm5hbWVzIjogW10KfQo=
