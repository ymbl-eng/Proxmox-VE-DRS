import time
from proxmoxer import ProxmoxAPI
from urllib3.exceptions import InsecureRequestWarning
import warnings

# InsecureRequestWarningの無視
warnings.simplefilter('ignore', InsecureRequestWarning)

# Proxmoxサーバの接続情報
proxmox_host = '192.168.0.1'
user = 'root@pam'
api_token_id = 'pve-token'
api_token_secret = '********-xxxx-yyyyy-zzzz-************'
verify_ssl = False

# Proxmox APIに接続
proxmox = ProxmoxAPI(proxmox_host, user=user, token_name=api_token_id, token_value=api_token_secret, verify_ssl=verify_ssl, service='PVE')

# 設定パラメータ
CHECK_INTERVAL = 300  # 負荷チェックの間隔（秒）
LOAD_THRESHOLD = 20  # 負荷不均衡とみなす閾値（CPUとメモリの合計使用率の差が20%以上）
MEMORY_THRESHOLD = 95  # 移動先ノードのメモリ使用率の上限
AUTO_MIGRATION = True  # 自動移動を有効にするかどうか
TARGET_NODES = ['pve1', 'pve2', 'pve3']  # DRSを動作させる対象ノードのリスト

def get_node_status():
    """ノードの状態を取得"""
    print("ノードの状態を取得中...")
    nodes = proxmox.nodes.get()
    node_status = {}
    for node in nodes:
        node_name = node['node']
        if node_name in TARGET_NODES:
            status = proxmox.nodes(node_name).status.get()
            memory_total = status['memory'].get('total', 1)  # メモリの総量
            memory_used = status['memory'].get('used', 0)   # 使用中のメモリ
            node_status[node_name] = {
                'cpu': round(status.get('cpu', 0) * 100, 2),
                'memory': {
                    'used': memory_used,
                    'total': memory_total,
                    'usage': round(memory_used / memory_total * 100, 2)  # メモリ使用率
                },
                'vm_list': [vm for vm in proxmox.nodes(node_name).qemu.get() if vm.get('status') != 'stopped']
            }
        else:
            print(f"{node_name}は対象ノードではありません。")
    return node_status

def get_vm_migration_candidate(node_status):
    """移動候補のVMを選定"""
    print("移動候補のVMを選定中...")

    highest_load_node, lowest_load_node = None, None
    highest_load_score, lowest_load_score = 0, float('inf')

    for node, status in node_status.items():
        cpu_usage = status['cpu']
        memory_usage = status['memory']['usage']
        load_score = cpu_usage + memory_usage
        if load_score > highest_load_score:
            highest_load_score = load_score
            highest_load_node = node
        if load_score < lowest_load_score:
            lowest_load_score = load_score
            lowest_load_node = node
    
    # ノード間の負荷差がLOAD_THRESHOLDを超えている場合のみ移動を実行
    if highest_load_score - lowest_load_score >= LOAD_THRESHOLD:
        print(f"高負荷ノード: {highest_load_node} (CPU使用率: {node_status[highest_load_node]['cpu']}%, メモリ使用率: {node_status[highest_load_node]['memory']['usage']}%)")
        print(f"低負荷ノード: {lowest_load_node} (CPU使用率: {node_status[lowest_load_node]['cpu']}%, メモリ使用率: {node_status[lowest_load_node]['memory']['usage']}%)")

        # VMリストの中からCPUおよびメモリの総使用率が軽い順に並べる
        vm_candidates = sorted(node_status[highest_load_node]['vm_list'], key=lambda vm: vm.get('cpu', 0) + (vm.get('mem', 0) / node_status[highest_load_node]['memory']['total'] * 100))
        for vm in vm_candidates:
            vm_memory = vm.get('mem', 0)
            vm_cpu = vm.get('cpu', 0)
            target_memory_available = node_status[lowest_load_node]['memory']['total'] - node_status[lowest_load_node]['memory']['used']
            
            # 移動後のリソース利用状況をシミュレーション
            if vm_memory <= target_memory_available:
                projected_cpu_usage = node_status[lowest_load_node]['cpu'] + (vm_cpu * 100)
                projected_memory_usage_pct = (node_status[lowest_load_node]['memory']['used'] + vm_memory) / node_status[lowest_load_node]['memory']['total'] * 100

                # 移動後のメモリ使用率が閾値以下であることを確認
                if projected_memory_usage_pct < MEMORY_THRESHOLD:
                    return highest_load_node, lowest_load_node, vm['vmid']

    print("負荷不均衡はありません。")
    return None, None, None

def migrate_vm(source_node, target_node, vmid):
    """VMを移動する"""
    if AUTO_MIGRATION:
        print(f"VMID {vmid}を{source_node}から{target_node}に移動中...")
        proxmox.nodes(source_node).qemu(vmid).migrate.post(target=target_node, online=1)
        print(f"VMID {vmid}を{source_node}から{target_node}に移動しました。")
    else:
        print(f"VMID {vmid}を{source_node}から{target_node}に移動することを推奨します。")
        print(f"推奨操作: proxmox.nodes('{source_node}').qemu({vmid}).migrate.post(target='{target_node}', online=1)")

def main():
    """DRSロジックのメインループ"""
    while True:
        print("DRSロジックを実行中...")
        node_status = get_node_status()
        source_node, target_node, vmid = get_vm_migration_candidate(node_status)
        if source_node and target_node and vmid:
            migrate_vm(source_node, target_node, vmid)
        else:
            print("移動するVMはありません。")
        print(f"{CHECK_INTERVAL}秒後に再度チェックします...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
