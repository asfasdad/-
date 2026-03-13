# Walmart 上架表格自动填写 MVP

这个服务支持：
- 上传沃尔玛模板表格（`template_file`）
- 上传产品基础信息表（`product_file`）
- 自动按列名匹配并填写数据
- 可选开启 AI 语义匹配（理解字段语义后决定该填哪一列）
- 返回填写后的 Excel（保留模板原有样式/公式结构）

## 1. 安装

```bash
pip install -r requirements.txt
```

## 2. 启动

```bash
uvicorn app:app --reload --port 8000
```

## 3. 调用接口

- `GET /health`
- `POST /autofill`
- `POST /learn-rules-from-folder`
- `POST /ai-connection-test`
- `POST /ai-models`
- `POST /ai-models-aggregate`
- `POST /opencode-auth/start`
- `GET /opencode-auth/status`
- `GET /opencode-models`
- `POST /opencode-model-connect`

`POST /autofill` 需要两个表单文件字段：
- `template_file`: 沃尔玛上架模板 `.xlsx`
- `product_file`: 产品基础信息 `.xlsx`

可选第三个字段：
- `mapping_file`: 映射规则 `.json`（上传则优先使用上传规则，不上传则自动加载默认规则 `mapping_rules.default.json`）

可选表单参数：
- `use_ai`: `true/false`，开启 AI 语义映射（默认 `false`）
- `ai_provider`: `openai / codex / deepseek / kimi`（默认 `openai`）
- `ai_model`: AI 模型名（默认 `gpt-4o-mini`）
- `ai_api_key`: 本次请求临时 API Key（优先于环境变量，不落盘）
- `ai_base_url`: 本次请求临时 Base URL（可选）

示例：

```bash
curl -X POST "http://127.0.0.1:8000/autofill" \
  -F "template_file=@./template.xlsx" \
  -F "product_file=@./products.xlsx" \
  -o "filled_walmart_template.xlsx"
```

带映射规则：

```bash
curl -X POST "http://127.0.0.1:8000/autofill" \
  -F "template_file=@./template.xlsx" \
  -F "product_file=@./products.xlsx" \
  -F "mapping_file=@./mapping_rules.example.json" \
  -o "filled_walmart_template.xlsx"
```

开启 AI：

```bash
curl -X POST "http://127.0.0.1:8000/autofill" \
  -F "template_file=@./template.xlsx" \
  -F "product_file=@./products.xlsx" \
  -F "use_ai=true" \
  -F "ai_provider=openai" \
  -F "ai_model=gpt-4o-mini" \
  -o "filled_walmart_template.xlsx"
```

返回头包含：
- `X-AI-Provider`: 实际使用的 provider
- `X-AI-Model`: 实际请求的模型名
- `X-AI-Effective-Provider`: 实际执行生成时使用的 provider
- `X-AI-Effective-Model`: 实际执行生成时使用的模型
- `X-AI-Route-Mode`: 路由模式（`direct` / `direct-key` / `fallback-openai-env` / `fallback-deepseek-env` / `fallback-kimi-env`）
- `X-Rules-Source`: 规则来源（`uploaded` / `default` / `none`）
- `X-Mapped-Columns`: 成功映射的列数
- `X-Unmapped-Columns`: 未映射列名（最多 20 个）
- `X-Required-Unfilled`: 规则标记必填但仍未命中的列
- `X-Unresolved-AI`: AI 返回但无法落地的映射（最多 20 个）
- `X-Unresolved-Rules`: 规则文件中无法解析的映射（最多 20 个）
- `X-AI-Warning`: AI 调用失败或解析失败时的提示（失败会自动降级到非 AI 匹配）
- `X-Filled-Rows`: 成功写入的行数

## AI 模式配置

使用 AI 时按 provider 配置环境变量：

```bash
set OPENAI_API_KEY=你的key
```

可选：

```bash
set OPENAI_BASE_URL=https://api.openai.com/v1
```

DeepSeek：

```bash
set DEEPSEEK_API_KEY=你的key
set DEEPSEEK_BASE_URL=https://api.deepseek.com/v1
```

Kimi（Moonshot）：

```bash
set KIMI_API_KEY=你的key
set KIMI_BASE_URL=https://api.moonshot.ai/v1
```

说明：`codex` provider 使用 OpenAI 兼容接口（与 `OPENAI_API_KEY` / `OPENAI_BASE_URL` 相同）。

注意：本工具当前不支持“网页 OAuth 一键登录”到供应商账号（包括 Codex）。
推荐在页面中粘贴 API Key，或使用环境变量配置。

补充：Codex 网页 OAuth 主要用于登录和模型确认。正式填表生成时，如果选择 `codex` 但未提供 API Key，系统会自动尝试路由到稳定通道（环境变量中的 OpenAI/DeepSeek/Kimi）。

补充：已接入 OpenCode 授权向导桥接。
- 点击网页里的“网页授权登录 Codex”会拉起本机 `opencode providers login`。
- 在弹出终端中选择 OpenAI 后，会出现授权 URL，浏览器确认后即可完成 OAuth。
- 点击“检查 Codex 授权状态”可确认是否已授权。

## 测试 AI 是否连通

```bash
curl -X POST "http://127.0.0.1:8000/ai-connection-test" \
  -F "ai_provider=deepseek" \
  -F "ai_model=deepseek-chat"
```

返回 `success=true` 表示连通；否则会返回 `error`（常见是 key 未配置或 provider 参数不兼容）。

## 拉取可用模型

```bash
curl -X POST "http://127.0.0.1:8000/ai-models" \
  -F "ai_provider=kimi" \
  -F "ai_api_key=你的key"
```

会返回 `models` 列表。若 provider 暂不支持远程拉取或 key 缺失，会返回预置模型列表。

## 合并拉取多供应商模型

```bash
curl -X POST "http://127.0.0.1:8000/ai-models-aggregate" \
  -F "openai_api_key=你的openai_key" \
  -F "deepseek_api_key=你的deepseek_key" \
  -F "kimi_api_key=你的kimi_key" \
  -F "include_codex_oauth=true"
```

返回 `merged_models`（如 `openai/gpt-4o-mini`、`deepseek/deepseek-chat`、`codex/gpt-5-codex`），前端可直接统一下拉选择。

## 命令行模型模式（简化 UI）

- `GET /opencode-models`：通过命令行 `opencode models` 拉模型列表。
- `POST /opencode-model-connect`：对选中的 `provider/model` 做连接测试。
- `POST /autofill` 支持 `ai_model_full=provider/model`，前端无需单独选择 provider。

映射逻辑（规则 + AI 结合）：
1. 自动匹配（别名+模糊）
2. 规则约束生效（上传规则优先，否则默认规则）：
   - `mode=force`：按规则源列强制填写
   - `mode=skip`：该模板字段跳过，不填写
   - `required=true`：标记为必填，若最终未命中会出现在 `X-Required-Unfilled`
   - `allow_ai=false`：该字段不允许 AI 补全
3. AI 语义匹配（`use_ai=true`）：只补充还没映射且 `allow_ai=true` 的字段

## 从已填写样例自动提取规则

你可以把多个“填写好的表格”放在 `填写完成的表格` 目录，然后调用：

```bash
curl -X POST "http://127.0.0.1:8000/learn-rules-from-folder" \
  -F "completed_dir=填写完成的表格" \
  -F "min_support=2" \
  -F "save_as_default=true" \
  -o "learned_mapping_rules.json"
```

如果想用指定产品信息表（不使用默认 `沃尔玛产品信息表.xlsx`）：

```bash
curl -X POST "http://127.0.0.1:8000/learn-rules-from-folder" \
  -F "product_file=@./沃尔玛产品信息表.xlsx" \
  -F "completed_dir=填写完成的表格" \
  -F "min_support=2" \
  -F "save_as_default=true" \
  -o "learned_mapping_rules.json"
```

默认规则文件位置：`mapping_rules.default.json`（在项目根目录）。

## 规则文件格式

支持两种：

1) 推荐格式（可带约束）：

```json
{
  "mappings": [
    {
      "template": "Product Name",
      "source": "商品名称",
      "mode": "force",
      "required": true,
      "allow_ai": true
    },
    {
      "template": "Seller SKU",
      "source": "SKU",
      "mode": "force",
      "required": true,
      "allow_ai": false
    },
    {
      "template": "Some Optional Field",
      "mode": "skip",
      "required": false,
      "allow_ai": false
    }
  ]
}
```

2) 简化格式：

```json
{
  "Product Name": "商品名称",
  "Seller SKU": "SKU"
}
```

## 4. 当前匹配策略

1. 列名标准化（大小写、空格、符号）
2. 别名匹配（中英文常见字段：`sku`、`price`、`brand` 等）
3. 模糊匹配（`difflib`）

## 6. 下一步建议

- 增加“列映射配置文件”功能（每个店铺一套固定映射）
- 增加“校验报告 sheet”（比如必填项缺失、价格为空）
- 增加多 sheet 指定和批量处理
