apiVersion: batch/v1
kind: Job
metadata:
  name: ngkg-performance-qualification
  namespace: ngkg-system
  labels:
    app.kubernetes.io/name: ngkg-performance-qualification
    kueue.x-k8s.io/queue-name: ngkg-performance
spec:
  completionMode: Indexed
  completions: ${NGKG_BENCHMARK_PARTITIONS}
  parallelism: 1
  backoffLimitPerIndex: 0
  maxFailedIndexes: 0
  ttlSecondsAfterFinished: 604800
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ngkg-performance-qualification
        ngkg.io/workload: sparql-query-processing
    spec:
      restartPolicy: Never
      serviceAccountName: ngkg-performance-qualification
      automountServiceAccountToken: false
      priorityClassName: ngkg-benchmark
      nodeSelector:
        ngkg.io/benchmark-hardware: phase40-13-23
      securityContext:
        runAsNonRoot: true
        seccompProfile: {type: RuntimeDefault}
      containers:
      - name: qualification
        image: ${NGKG_QUALIFICATION_IMAGE_REPOSITORY}@sha256:${NGKG_QUALIFICATION_IMAGE_SHA256}
        imagePullPolicy: IfNotPresent
        args:
        - scripts/run_phase40_13_23_partition.py
        - --plan=/evidence/plan.json
        - --inventory=/evidence/inventory.yaml
        - --catalog=/evidence/catalog.json
        - --pricing=/evidence/pricing.json
        - --output=/reports/partition-$(JOB_COMPLETION_INDEX).json
        - --ngkg-driver=/opt/c8/bin/ngkg-rust-performance-driver
        - --external-jena-driver=/opt/c8/bin/external-jena-client
        env:
        - name: JOB_COMPLETION_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
        - {name: OMP_NUM_THREADS, value: '1'}
        - {name: OPENBLAS_NUM_THREADS, value: '1'}
        - {name: MKL_NUM_THREADS, value: '1'}
        - {name: NUMEXPR_NUM_THREADS, value: '1'}
        resources:
          requests: {cpu: '4', memory: 16Gi}
          limits: {cpu: '4', memory: 16Gi}
        securityContext:
          allowPrivilegeEscalation: false
          capabilities: {drop: [ALL]}
          readOnlyRootFilesystem: true
        volumeMounts:
        - {name: evidence, mountPath: /evidence, readOnly: true}
        - {name: reports, mountPath: /reports}
        - {name: baseline-client, mountPath: /opt/c8/bin/external-jena-client, subPath: external-jena-client, readOnly: true}
        - {name: tmp, mountPath: /tmp}
      volumes:
      - name: evidence
        projected:
          sources:
          - configMap: {name: ngkg-performance-plan}
          - secret: {name: ngkg-performance-evidence}
      - name: reports
        persistentVolumeClaim: {claimName: ngkg-performance-reports}
      - name: baseline-client
        secret:
          secretName: ngkg-external-baseline-client
          defaultMode: 0550
      - name: tmp
        emptyDir: {sizeLimit: 2Gi}
