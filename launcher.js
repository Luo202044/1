const { startObscura } = require('@chegger/node-obscura');
const { spawn } = require('child_process');

async function main() {
  console.log("🚀 [原生基座] 正在拉起极速无头内核 (Native Binary)...");
  
  // 启动内核，开启反指纹，它会自动在宿主机裸跑，不经过任何 Docker 网络！
  const obscura = await startObscura({ stealth: true });
  console.log(`✅ [原生基座] 内核已成功启动！`);
  console.log(`🔗 动态内部直连端点: ${obscura.endpoint}`);

  // 将 WebSocket 端点通过环境变量传递给 Python
  const env = Object.assign({}, process.env, { CDP_URL: obscura.endpoint });

  console.log("🐍 [原生基座] 正在挂载并拉起 Python 爬虫引擎...\n");
  console.log("=====================================================");
  
  // 拉起 Python，继承标准输出（让你在 GitHub Actions 能看到实时日志）
  const pyProcess = spawn('python', ['scanner_nextgen.py'], { env: env, stdio: 'inherit' });

  pyProcess.on('close', async (code) => {
    console.log("=====================================================");
    console.log(`🛑 [原生基座] Python 引擎已退出，退出码: ${code}`);
    console.log("🧹 [原生基座] 正在安全销毁底层内核...");
    await obscura.close(); // 清理进程
    process.exit(code);
  });
}

main().catch(err => {
  console.error("❌ 启动失败:", err);
  process.exit(1);
});
