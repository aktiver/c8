apiVersion: batch/v1
kind: Job
metadata:
  name: ngkg-release-qualification-hpc
  namespace: ngkg-release-qualification
  labels:
    app.kubernetes.io/name: ngkg-release-qualification
    kueue.x-k8s.io/queue-name: ngkg-release-qualification
spec:
  completionMode: Indexed
  completions: ${NGKG_RELEASE_PARTITIONS}
  parallelism: ${NGKG_RELEASE_PARALLELISM}
  backoffLimitPerIndex: 0
  maxFailedIndexes: 0
  ttlSecondsAfterFinished: 604800
  template:
    metadata:
      labels: {app.kubernetes.io/name: ngkg-release-qualification, ngkg.io/workload: release-qualification}
    spec:
      restartPolicy: Never
      serviceAccountName: ngkg-release-qualification
      automountServiceAccountToken: false
      priorityClassName: ngkg-qualification
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector: {matchLabels: {app.kubernetes.io/name: ngkg-release-qualification}}
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - topologyKey: kubernetes.io/hostname
            labelSelector: {matchLabels: {app.kubernetes.io/name: ngkg-release-qualification}}
      securityContext: {runAsNonRoot: true, seccompProfile: {type: RuntimeDefault}}
      containers:
      - name: qualification
        image: ${NGKG_QUALIFICATION_IMAGE_REPOSITORY}@sha256:${NGKG_QUALIFICATION_IMAGE_SHA256}
        imagePullPolicy: IfNotPresent
        args: [scripts/run_phase40_13_24_partition.py, --plan=/evidence/plan.json, --catalog=/evidence/catalog.json, --partition=$(JOB_COMPLETION_INDEX), --worker-id=$(POD_UID), --driver=/opt/c8/bin/ngkg-release-driver, --output=/reports/partition-$(JOB_COMPLETION_INDEX).json]
        env:
        - name: JOB_COMPLETION_INDEX
          valueFrom: {fieldRef: {fieldPath: "metadata.annotations['batch.kubernetes.io/job-completion-index']"}}
        - name: POD_UID
          valueFrom: {fieldRef: {fieldPath: metadata.uid}}
        - {name: NGKG_RELEASE_WORKER_THREADS, value: '8'}
        - {name: OMP_NUM_THREADS, value: '1'}
        - {name: OPENBLAS_NUM_THREADS, value: '1'}
        - {name: MKL_NUM_THREADS, value: '1'}
        resources:
          requests: {cpu: '8', memory: 32Gi}
          limits: {cpu: '8', memory: 32Gi}
        securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}, readOnlyRootFilesystem: true}
        volumeMounts:
        - {name: evidence, mountPath: /evidence, readOnly: true}
        - {name: reports, mountPath: /reports}
        - {name: tmp, mountPath: /tmp}
      volumes:
      - name: evidence
        projected: {sources: [{configMap: {name: ngkg-release-plan}}, {secret: {name: ngkg-release-inputs}}]}
      - name: reports
        persistentVolumeClaim: {claimName: ngkg-release-reports}
      - name: tmp
        emptyDir: {sizeLimit: 8Gi}
