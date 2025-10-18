import time
import random
import csv
from HLLSwitchServer import HLLSwitchServer
from collections import defaultdict

from sklearn.metrics import confusion_matrix, classification_report

def read_caida_packets(filepath):
    with open(filepath, 'r') as f:
        for line in f:
            if '\t' in line:
                src, dst = line.strip().split('\t')
                yield src, dst

def main():
    total_switches = 1 # we start with distributed processing, you can ignore this setting and the corresponding port assignment in the rest of this code
    hll_ratio = 0.6 # this is the ratio of DR
    hll_bits_per_register = 3 # which is b in the paper, the 
    cmscu_depth = 3 #k_c of CU (or CC)
    bs_entries = 3 #k in the paper
    total_memory_bits =240*1024 
    SUPER_RECEIVER_THRESHOLD = 400 #threshold for super spreaders

    # HLL memory
    hll_bits = int(total_memory_bits * hll_ratio)

    # cmscu_bits = total_memory_bits - hll_bits
    # + CU memory
    bs_cu_bits = total_memory_bits - hll_bits # the total memory for SS Keeper and CC
    bs_ratio = 0.05 # the memory ratio for SS Keeper, we can adjust it based on the totla memory for a fixed number entries in about 200.

    # DR register setup
    m = hll_bits // hll_bits_per_register
    m = m - (m % total_switches)
    segment_size = m // total_switches#this is not needed in the paper

    # SS Keeper setup
    # cmscu_width = cmscu_bits // (cmscu_depth * 32)
    bs_bits = int(total_memory_bits * bs_ratio)
    bs_width = bs_bits // (bs_entries * 32)  # 4 entries per bucket, 32 + 32  bits each

    # CC (or CU sketch) memory and width
    cu_bits = bs_cu_bits - bs_bits
    cmscu_width = cu_bits // (cmscu_depth * 8)

    print(f"Total Memory (bits): {total_memory_bits}")
    print(f"HLL Registers (m): {m}, Segment Size: {segment_size}")
    print(f"CMSCU Width: {cmscu_width}, Depth: {cmscu_depth}")
    print(f"BS Width: {bs_width}, Depth: {bs_entries}")


    # Port assignments for switches
    port_map = {
        0: ('127.0.0.1', 10001),
        1: ('127.0.0.1', 10002),
        2: ('127.0.0.1', 10003)
    }

    print(f"Setting up {total_switches} switches with m={m}, segment_size={segment_size}")
    switches = []
    for i in range(total_switches):
        switch = HLLSwitchServer(  #HLLSwitchServer takes the input data set and performa super spreader identification.
            switch_id=i,
            total_switches=total_switches,
            m=m,
            port=port_map[i][1],
            cmscu_width=cmscu_width,
            cmscu_depth=cmscu_depth,
            bs_width=bs_width,
            bs_entries=bs_entries,
            port_map=port_map,
            ssthreshold = SUPER_RECEIVER_THRESHOLD,
        )
        switches.append(switch)

    # Ground truth for reverse spread (dst → set of srcs)
    ground_truth = defaultdict(set)

    # Load CAIDA file and simulate packet sending
    packet_set = set(read_caida_packets("events.txt"))
    print(f" Loaded {len(packet_set)} packets")
    packet_list = list(packet_set)
    print(f" Loaded {len(packet_list)} unique packets after deduplication")

    for src, dst in packet_list:
        ground_truth[dst].add(src)
        assigned = random.randint(0, total_switches - 1)
        switches[assigned].receive_packet(src, dst)

    # Allow time for forwarding
    time.sleep(2)

    # Gather and merge estimates
    final_estimates = defaultdict(int)
    for dst in ground_truth:
        est = sum(s.query(dst) for s in switches)
        final_estimates[dst] = est

    # Write to CSV
    with open("reverse_spread_estimates.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["DestinationIP", "EstimatedSpread", "GroundTruth"])
        for dst in ground_truth:
            writer.writerow([dst, final_estimates[dst], len(ground_truth[dst])])

    # Print summary
    print("\n📊 Summary Stats:")
    for s in switches:
        print(s.stats())
    print("Estimates saved to reverse_spread_estimates.csv")

    for i, s in enumerate(switches):
        s.topkcuckoo.dump_buckets(f"bubblesketch_switch_{i}.txt")



    y_true = []
    y_pred = []

    for dst in ground_truth:
        gt = len(ground_truth[dst])
        est = final_estimates[dst]
        
        # Ground truth: 1 if actual spread ≥ threshold
        y_true.append(1 if gt >= SUPER_RECEIVER_THRESHOLD else 0)
        
        # Prediction: 1 if estimated spread ≥ threshold
        y_pred.append(1 if est >= SUPER_RECEIVER_THRESHOLD else 0)

    # Generate confusion matrix
    cm = confusion_matrix(y_true, y_pred)
    report = classification_report(y_true, y_pred, target_names=["Non-SuperSpreader", "SuperSpreader"])

    print("\nConfusion Matrix:")
    print(cm)
    print("\nClassification Report:")
    print(report)

    false_negatives = []
    false_positives = []

    for dst in ground_truth:
        gt = len(ground_truth[dst])
        est = final_estimates[dst]
        
        actual_label = 1 if gt >= SUPER_RECEIVER_THRESHOLD else 0
        predicted_label = 1 if est >= SUPER_RECEIVER_THRESHOLD else 0

        if actual_label == 1 and predicted_label == 0:
            false_negatives.append((dst, est, gt))
        elif actual_label == 0 and predicted_label == 1:
            false_positives.append((dst, est, gt))

    print("\n False Negatives (Missed Super Receivers):")
    #for dst, est, gt in false_negatives:
    #    print(f"  {dst}: Estimated = {est}, Actual = {gt}")

    print("\nFalse Positives (Wrongly Detected Super Receivers):")
    #for dst, est, gt in false_positives:
    #    print(f"  {dst}: Estimated = {est}, Actual = {gt}")


if __name__ == "__main__":
    main()
