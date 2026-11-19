// @types/react-syntax-highlighter 只声明了包根模块，语言包（dist/esm/languages/prism/*）
// 没有类型。这里按需给默认导出一个宽松类型，匹配 registerLanguage(name, func: any) 的签名。
declare module "react-syntax-highlighter/dist/esm/languages/prism/*" {
  const language: unknown;
  export default language;
}
