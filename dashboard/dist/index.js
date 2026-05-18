/**
 * hermes-agent-cluster Dashboard Plugin
 *
 * Hermes Plugin SDK Component for the Cluster Management tab.
 * Includes Dashboard view + Configuration panel.
 *
 * Register with: window.__HERMES_PLUGINS__.register('agent-cluster', Component)
 */
(function () {
  'use strict';

  var SDK = window.__HERMES_PLUGIN_SDK__;
  if (!SDK) {
    console.warn('[agent-cluster] Plugin SDK not found — skipping registration');
    return;
  }

  var React = SDK.React;
  var h = React.createElement;
  var hooks = SDK.hooks;
  var comp = SDK.components;
  var utils = SDK.utils;
  var fetchJSON = SDK.fetchJSON;

  // -----------------------------------------------------------------------
  // Styles
  // -----------------------------------------------------------------------

  var s = {
    grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px', marginBottom: '20px' },
    stat: { fontSize: '1.8rem', fontWeight: 600, lineHeight: 1.2 },
    statLabel: { fontSize: '0.7rem', textTransform: 'uppercase', letterSpacing: '0.08em', opacity: 0.6, marginTop: '4px' },
    table: { width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' },
    th: { textAlign: 'left', padding: '8px 10px', borderBottom: '1px solid rgba(255,255,255,0.08)', fontWeight: 500, textTransform: 'uppercase', fontSize: '0.65rem', letterSpacing: '0.08em', opacity: 0.5 },
    td: { padding: '8px 10px', borderBottom: '1px solid rgba(255,255,255,0.04)', verticalAlign: 'middle' },
    badge: { display: 'inline-block', padding: '2px 8px', borderRadius: '4px', fontSize: '0.7rem', fontWeight: 500 },
    tabBar: { display: 'flex', gap: '0px', marginBottom: '20px', borderBottom: '1px solid rgba(255,255,255,0.08)' },
    tab: { padding: '8px 16px', cursor: 'pointer', fontSize: '0.8rem', borderBottom: '2px solid transparent', opacity: 0.5, transition: 'all 0.15s' },
    tabActive: { padding: '8px 16px', cursor: 'pointer', fontSize: '0.8rem', borderBottom: '2px solid #3b82f6', opacity: 1, fontWeight: 600 },
    section: { marginBottom: '16px' },
    label: { fontSize: '0.7rem', textTransform: 'uppercase', letterSpacing: '0.08em', opacity: 0.5, marginBottom: '6px' },
    value: { fontSize: '0.85rem' },
    chip: { display: 'inline-block', padding: '2px 8px', margin: '2px 4px 2px 0', borderRadius: '4px', fontSize: '0.75rem', background: 'rgba(255,255,255,0.06)', cursor: 'pointer' },
    chipActive: { display: 'inline-block', padding: '2px 8px', margin: '2px 4px 2px 0', borderRadius: '4px', fontSize: '0.75rem', background: 'rgba(59,130,246,0.2)', color: '#3b82f6', cursor: 'pointer' },
    chipAdd: { display: 'inline-block', padding: '2px 8px', margin: '2px 4px 2px 0', borderRadius: '4px', fontSize: '0.75rem', background: 'rgba(34,197,94,0.15)', color: '#22c55e', cursor: 'pointer', border: '1px dashed rgba(34,197,94,0.3)' },
    input: { width: '100%', padding: '6px 10px', borderRadius: '4px', border: '1px solid rgba(255,255,255,0.1)', background: 'rgba(0,0,0,0.2)', color: 'inherit', fontSize: '0.8rem', outline: 'none' },
    infoRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '8px 0', borderBottom: '1px solid rgba(255,255,255,0.04)' },
    restartBanner: { padding: '10px 14px', borderRadius: '6px', background: 'rgba(234,179,8,0.1)', color: '#eab308', fontSize: '0.8rem', marginBottom: '12px' },
  };

  var badgeStyle = function (type, value) {
    var base = Object.assign({}, s.badge);
    var colors = {
      online: { bg: 'rgba(34,197,94,0.15)', c: '#22c55e' },
      degraded: { bg: 'rgba(234,179,8,0.15)', c: '#eab308' },
      offline: { bg: 'rgba(239,68,68,0.15)', c: '#ef4444' },
      completed: { bg: 'rgba(34,197,94,0.15)', c: '#22c55e' },
      running: { bg: 'rgba(139,92,246,0.15)', c: '#8b5cf6' },
      assigned: { bg: 'rgba(139,92,246,0.15)', c: '#8b5cf6' },
      ready: { bg: 'rgba(59,130,246,0.15)', c: '#3b82f6' },
      pending: { bg: 'rgba(59,130,246,0.15)', c: '#3b82f6' },
      blocked: { bg: 'rgba(234,179,8,0.15)', c: '#eab308' },
      failed: { bg: 'rgba(239,68,68,0.15)', c: '#ef4444' },
    };
    var col = colors[value] || { bg: 'rgba(255,255,255,0.08)', c: '#888' };
    base.background = col.bg;
    base.color = col.c;
    return base;
  };

  // -----------------------------------------------------------------------
  // Sub-components
  // -----------------------------------------------------------------------

  function Badge(type, value) {
    return h('span', { style: badgeStyle(type, value) }, value || '—');
  }

  function CapChips(caps, onClick, active) {
    if (!caps || caps.length === 0) return h('span', { style: { opacity: 0.4, fontSize: '0.7rem' } }, '—');
    return h('span', null,
      caps.map(function (c) {
        var isActive = active && active.indexOf(c) >= 0;
        return h('span', {
          key: c,
          style: isActive ? s.chipActive : s.chip,
          onClick: onClick ? function () { onClick(c); } : undefined,
          title: onClick ? 'Click to toggle' : undefined,
        }, c);
      })
    );
  }

  function InfoRow(label, value, extra) {
    return h('div', { style: s.infoRow },
      h('span', { style: { fontSize: '0.75rem', opacity: 0.6 } }, label),
      h('span', { style: { fontSize: '0.8rem', display: 'flex', alignItems: 'center', gap: '6px' } },
        value || h('span', { style: { opacity: 0.4 } }, '—'),
        extra || null,
      ),
    );
  }

  function StatCard(label, value, color) {
    return h(comp.Card, null,
      h(comp.CardContent, null,
        h('div', { style: { padding: '4px 0' } },
          h('div', { style: Object.assign({}, s.stat, color ? { color: color } : {}) },
            value === null || value === undefined ? '—' : (typeof value === 'number' ? value.toLocaleString() : String(value))
          ),
          h('div', { style: s.statLabel }, label)
        )
      )
    );
  }

  // -----------------------------------------------------------------------
  // Config Panel
  // -----------------------------------------------------------------------

  function ConfigPanel(props) {
    var _useState = hooks.useState({
      config: null,
      loading: false,
      saved: false,
      error: null,
      newCap: '',
      restarting: false,
    });
    var state = state[0];
    var setState = state[1];
    var nodeConfig = state.config;

    var refresh = hooks.useCallback(function () {
      setState(function (p) { return Object.assign({}, p, { loading: true, error: null }); });
      fetchJSON('/api/plugins/agent-cluster/config/node')
        .then(function (data) {
          setState(function (p) { return Object.assign({}, p, { config: data, loading: false, saved: false }); });
        })
        .catch(function (err) {
          setState(function (p) { return Object.assign({}, p, { loading: false, error: err.message }); });
        });
    }, []);

    hooks.useEffect(function () { refresh(); }, []);

    var toggleCap = function (cap) {
      if (!nodeConfig || !nodeConfig.node) return;
      var current = nodeConfig.node.capabilities || [];
      var next;
      if (current.indexOf(cap) >= 0) {
        next = current.filter(function (c) { return c !== cap; });
      } else {
        next = current.concat([cap]);
      }
      setState(function (p) { return Object.assign({}, p, { config: Object.assign({}, p.config, { node: Object.assign({}, p.config.node, { capabilities: next }) }) }); });
    };

    var addCustomCap = function () {
      if (!state.newCap.trim()) return;
      toggleCap(state.newCap.trim());
      setState(function (p) { return Object.assign({}, p, { newCap: '' }); });
    };

    var saveCapabilities = function () {
      if (!nodeConfig || !nodeConfig.node) return;
      var caps = nodeConfig.node.capabilities || [];
      setState(function (p) { return Object.assign({}, p, { loading: true, error: null }); });
      fetchJSON('/api/plugins/agent-cluster/config/capabilities', {
        method: 'PUT',
        body: JSON.stringify({ capabilities: caps }),
        headers: { 'Content-Type': 'application/json' },
      }).then(function (data) {
        setState(function (p) { return Object.assign({}, p, { loading: false, saved: true }); });
        setTimeout(function () { setState(function (p) { return Object.assign({}, p, { saved: false }); }); }, 3000);
        props.onRefresh();
      }).catch(function (err) {
        setState(function (p) { return Object.assign({}, p, { loading: false, error: err.message }); });
      });
    };

    var restartService = function () {
      setState(function (p) { return Object.assign({}, p, { restarting: true, error: null }); });
      fetchJSON('/api/plugins/agent-cluster/config/restart', { method: 'POST' })
        .then(function (data) {
          setState(function (p) { return Object.assign({}, p, { restarting: false }); });
          setTimeout(refresh, 2000);
        })
        .catch(function (err) {
          setState(function (p) { return Object.assign({}, p, { restarting: false, error: err.message }); });
        });
    };

    var needsRestart = state.saved;

    return h('div', null,

      // Header
      h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' } },
        h('div', null,
          h('h3', { style: { margin: 0, fontSize: '0.9rem', fontWeight: 600 } }, 'Configuration'),
          nodeConfig && nodeConfig.config_file
            ? h('div', { style: { fontSize: '0.65rem', opacity: 0.4, marginTop: '2px' } }, nodeConfig.config_file)
            : null,
        ),
        h(comp.Button, { size: 'sm', onClick: refresh, disabled: state.loading },
          state.loading ? '...' : '↻ Reload'
        ),
      ),

      // Error
      state.error ? h('div', { style: { padding: '10px 14px', borderRadius: '6px', background: 'rgba(239,68,68,0.1)', color: '#ef4444', fontSize: '0.85rem', marginBottom: '12px' } },
        '⚠ ', state.error,
      ) : null,

      // Saved banner
      needsRestart ? h('div', { style: s.restartBanner },
        '✅ Capabilities saved (runtime update may need a moment to propagate)'
      ) : null,

      // Not connected
      !state.loading && !nodeConfig ? h(comp.Card, null,
        h(comp.CardContent, { style: { textAlign: 'center', padding: '30px' } },
          h('div', { style: { opacity: 0.5, fontSize: '0.85rem' } }, 'Config file not found. Start the cluster service first.'),
        ),
      ) : null,

      // Config form
      nodeConfig ? h('div', { style: { display: 'flex', flexDirection: 'column', gap: '16px' } },

        // --- Node Identity ---
        h(comp.Card, null,
          h(comp.CardHeader, null,
            h(comp.CardTitle, { style: { fontSize: '0.85rem' } }, 'Node Identity'),
          ),
          h(comp.CardContent, null,
            InfoRow('Node ID', nodeConfig.node ? nodeConfig.node.id : '—'),
            InfoRow('Node Name', nodeConfig.node ? nodeConfig.node.name : '—'),
            InfoRow('Cluster Role', nodeConfig.cluster ? nodeConfig.cluster.role : '—'),
            InfoRow('Cluster ID', nodeConfig.cluster ? nodeConfig.cluster.id : '—'),
            InfoRow('Service Port', nodeConfig.server ? (nodeConfig.server.bind + ':' + nodeConfig.server.port) : '—'),
            nodeConfig.cluster && nodeConfig.cluster.role === 'worker' ? InfoRow('Main Endpoint', nodeConfig.cluster.endpoint || '—') : null,
          ),
        ),

        // --- Capabilities ---
        h(comp.Card, null,
          h(comp.CardHeader, null,
            h(comp.CardTitle, { style: { fontSize: '0.85rem' } }, 'Capabilities'),
            h('p', { style: { fontSize: '0.7rem', opacity: 0.5, margin: '4px 0 0' } },
              'Click to toggle. Changes take effect immediately at runtime and persist to config file.'
            ),
          ),
          h(comp.CardContent, null,
            // Preset capabilities
            h('div', { style: { marginBottom: '10px' } },
              h('div', { style: Object.assign({}, s.label, { marginBottom: '8px' }) }, 'Preset'),
              h('div', null,
                ['planning', 'reviewing', 'scheduling', 'coding', 'gpu', 'browser', 'research', 'testing', 'devops', 'writing'].map(function (cap) {
                  var current = (nodeConfig.node && nodeConfig.node.capabilities) || [];
                  var isActive = current.indexOf(cap) >= 0;
                  return h('span', {
                    key: cap,
                    style: isActive ? Object.assign({}, s.chipActive, { margin: '2px 4px 2px 0' }) : Object.assign({}, s.chip, { margin: '2px 4px 2px 0' }),
                    onClick: function () { toggleCap(cap); },
                    title: isActive ? 'Remove ' + cap : 'Add ' + cap,
                  }, cap);
                })
              ),
            ),
            // Custom cap input
            h('div', { style: { display: 'flex', gap: '6px', marginBottom: '12px' } },
              h('input', {
                style: Object.assign({}, s.input, { flex: 1 }),
                placeholder: 'Add custom capability...',
                value: state.newCap,
                onChange: function (e) { setState(function (p) { return Object.assign({}, p, { newCap: e.target.value }); }); },
                onKeyDown: function (e) { if (e.key === 'Enter') addCustomCap(); },
              }),
              h(comp.Button, { size: 'sm', variant: 'ghost', onClick: addCustomCap, disabled: !state.newCap.trim() }, '+ Add'),
            ),
            // Current capabilities
            h('div', null,
              h('div', { style: Object.assign({}, s.label, { marginBottom: '8px' }) }, 'Current (' + ((nodeConfig.node && nodeConfig.node.capabilities && nodeConfig.node.capabilities.length) || 0) + ')'),
              h('div', null,
                (nodeConfig.node && nodeConfig.node.capabilities && nodeConfig.node.capabilities.length > 0)
                  ? nodeConfig.node.capabilities.map(function (cap) {
                      return h('span', {
                        key: cap,
                        style: Object.assign({}, s.chipActive, { margin: '2px 4px 2px 0' }),
                        onClick: function () { toggleCap(cap); },
                        title: 'Remove ' + cap,
                      }, cap + ' ✕');
                    })
                  : h('span', { style: { opacity: 0.4, fontSize: '0.75rem' } }, 'No capabilities set'),
              ),
            ),
            // Save button
            h('div', { style: { marginTop: '12px' } },
              h(comp.Button, {
                size: 'sm',
                onClick: saveCapabilities,
                disabled: state.loading,
              }, state.loading ? 'Saving...' : '💾 Apply Capabilities'),
            ),
          ),
        ),

        // --- Runtime Status ---
        nodeConfig.runtime ? h(comp.Card, null,
          h(comp.CardHeader, null,
            h(comp.CardTitle, { style: { fontSize: '0.85rem' } }, 'Runtime Status'),
          ),
          h(comp.CardContent, null,
            InfoRow('Status', h('span', null, Badge('node', nodeConfig.runtime.status))),
            InfoRow('Load', (nodeConfig.runtime.load * 100).toFixed(0) + '%'),
            InfoRow('Last Heartbeat', nodeConfig.runtime.last_heartbeat ? (utils.isoTimeAgo ? utils.isoTimeAgo(nodeConfig.runtime.last_heartbeat) : nodeConfig.runtime.last_heartbeat) : '—'),
          ),
        ) : null,

        // --- Restart ---
        h(comp.Card, null,
          h(comp.CardHeader, null,
            h(comp.CardTitle, { style: { fontSize: '0.85rem' } }, 'Service Control'),
          ),
          h(comp.CardContent, null,
            h('p', { style: { fontSize: '0.75rem', opacity: 0.6, marginBottom: '10px' } },
              'Changes to role, port, or lease settings require a service restart.'
            ),
            h(comp.Button, {
              size: 'sm',
              variant: 'ghost',
              onClick: restartService,
              disabled: state.restarting,
            }, state.restarting ? 'Restarting...' : '🔄 Restart Cluster Service'),
          ),
        ),

      ) : null,
    );
  }

  // -----------------------------------------------------------------------
  // Main Dashboard Component
  // -----------------------------------------------------------------------

  function ClusterDashboard() {
    var _useState2 = hooks.useState('dashboard');
    var tab = _useState2[0];
    var setTab = _useState2[1];

    var _useState3 = hooks.useState({
      status: null, nodes: null, tasks: null, leases: null,
      config: null, loading: true, error: null,
      editEndpoint: false, endpointInput: '',
    });
    var state = _useState3[0];
    var setState = _useState3[1];

    var refresh = hooks.useCallback(function () {
      setState(function (p) { return Object.assign({}, p, { loading: true, error: null }); });
      var base = '/api/plugins/agent-cluster';
      Promise.all([
        fetchJSON(base + '/status').catch(function () { return null; }),
        fetchJSON(base + '/nodes').catch(function () { return null; }),
        fetchJSON(base + '/tasks').catch(function () { return null; }),
        fetchJSON(base + '/leases').catch(function () { return null; }),
        fetchJSON(base + '/config').catch(function () { return null; }),
      ]).then(function (results) {
        setState(function (p) {
          return Object.assign({}, p, {
            status: results[0], nodes: results[1], tasks: results[2],
            leases: results[3], config: results[4],
            loading: false, error: null,
            endpointInput: results[4] && results[4].endpoint ? results[4].endpoint : p.endpointInput,
          });
        });
      }).catch(function (err) {
        setState(function (p) { return Object.assign({}, p, { loading: false, error: err.message || 'Failed to load' }); });
      });
    }, []);

    hooks.useEffect(function () { refresh(); }, []);

    var saveEndpoint = function () {
      var ep = state.endpointInput;
      fetchJSON('/api/plugins/agent-cluster/config', {
        method: 'POST', body: JSON.stringify({ endpoint: ep }),
        headers: { 'Content-Type': 'application/json' },
      }).then(function (r) {
        setState(function (p) { return Object.assign({}, p, { editEndpoint: false, config: r }); });
        refresh();
      }).catch(function (err) { console.warn('[agent-cluster] Endpoint save error:', err); });
    };

    var isConnected = state.status && state.status.ok !== false;
    var summary = isConnected && state.status && state.status.summary ? state.status.summary : null;

    return h('div', { style: { display: 'flex', flexDirection: 'column', gap: '16px', padding: '8px 0' } },

      // === Tab Bar ===
      h('div', { style: s.tabBar },
        h('div', {
          style: tab === 'dashboard' ? s.tabActive : s.tab,
          onClick: function () { setTab('dashboard'); },
        }, '📊 Dashboard'),
        h('div', {
          style: tab === 'config' ? s.tabActive : s.tab,
          onClick: function () { setTab('config'); },
        }, '⚙ Config'),
      ),

      // === Config Tab ===
      tab === 'config' ? h(ConfigPanel, { onRefresh: refresh }) :

      // === Dashboard Tab ===
      h('div', null,

        // Header
        h('div', { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' } },
          h('div', null,
            h('h2', { style: { margin: 0, fontSize: '1.2rem', fontWeight: 600 } }, 'Cluster Dashboard'),
            state.config ? h('div', { style: { fontSize: '0.65rem', opacity: 0.4, marginTop: '2px' } },
              'Endpoint: ', state.config.endpoint || 'http://127.0.0.1:8787'
            ) : null,
          ),
          h('div', { style: { display: 'flex', gap: '8px', alignItems: 'center' } },
            state.editEndpoint
              ? h(React.Fragment, null,
                  h('input', {
                    style: Object.assign({}, s.input, { width: '200px' }),
                    value: state.endpointInput,
                    onChange: function (e) { setState(function (p) { return Object.assign({}, p, { endpointInput: e.target.value }); }); },
                    placeholder: 'http://host:port',
                  }),
                  h(comp.Button, { size: 'sm', onClick: saveEndpoint }, 'Save'),
                  h(comp.Button, { size: 'sm', variant: 'ghost', onClick: function () { setState(function (p) { return Object.assign({}, p, { editEndpoint: false }); }); } }, 'Cancel'),
                )
              : h(comp.Button, { size: 'sm', variant: 'ghost', onClick: function () { setState(function (p) { return Object.assign({}, p, { editEndpoint: true }); }); } }, '⚙ Configure'),
            h(comp.Button, { size: 'sm', onClick: refresh, disabled: state.loading },
              state.loading ? '↻ Loading...' : '↻ Refresh'
            ),
          ),
        ),

        // Error
        state.error ? h('div', { style: { padding: '12px 16px', borderRadius: '6px', background: 'rgba(239,68,68,0.1)', color: '#ef4444', fontSize: '0.85rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' } },
          h('span', null, '⚠ ', state.error),
          h(comp.Button, { size: 'sm', variant: 'ghost', onClick: function () { setState(function (p) { return Object.assign({}, p, { error: null }); }); } }, '✕'),
        ) : null,

        // Not connected
        !state.loading && !isConnected && !state.error ? h(comp.Card, null,
          h(comp.CardContent, { style: { textAlign: 'center', padding: '40px' } },
            h('div', { style: { fontSize: '2rem', marginBottom: '12px' } }, '🌐'),
            h('div', { style: { fontSize: '0.9rem', fontWeight: 600, marginBottom: '8px' } }, 'Cluster Service Not Connected'),
            h('div', { style: { fontSize: '0.8rem', opacity: 0.6, marginBottom: '16px' } }, 'Make sure hermes-cluster is running, then configure the endpoint.'),
            h(comp.Button, { size: 'sm', onClick: function () { setState(function (p) { return Object.assign({}, p, { editEndpoint: true }); }); } }, 'Configure Endpoint'),
          ),
        ) : null,

        // Connected: summary + tables
        isConnected ? h('div', null,

          // Summary cards
          h('div', { style: s.grid },
            StatCard('Nodes Online', summary ? (summary.online_nodes || 0) + ' / ' + (summary.total_nodes || 0) : '—', '#22c55e'),
            StatCard('Total Tasks', summary ? summary.total_tasks : state.tasks ? (Array.isArray(state.tasks) ? state.tasks.length : 0) : 0, '#3b82f6'),
            StatCard('Running', summary && summary.tasks_by_status ? ((summary.tasks_by_status.running || 0) + (summary.tasks_by_status.assigned || 0)) : 0, '#8b5cf6'),
            StatCard('Completed', summary && summary.tasks_by_status ? (summary.tasks_by_status.completed || 0) : 0, '#22c55e'),
            StatCard('Pending', summary && summary.tasks_by_status ? ((summary.tasks_by_status.pending || 0) + (summary.tasks_by_status.ready || 0)) : 0, '#3b82f6'),
            StatCard('Active Leases', summary ? summary.active_leases : (state.leases ? (Array.isArray(state.leases) ? state.leases.length : 0) : 0), '#eab308'),
          ),

          // Nodes table
          state.nodes && Array.isArray(state.nodes) && state.nodes.length > 0
            ? h(comp.Card, null,
                h(comp.CardHeader, null, h(comp.CardTitle, { style: { fontSize: '0.9rem' } }, 'Nodes (', state.nodes.length, ')')),
                h(comp.CardContent, { style: { padding: 0, overflowX: 'auto' } },
                  h('table', { style: s.table },
                    h('thead', null, h('tr', null,
                      h('th', { style: s.th }, 'Name'),
                      h('th', { style: s.th }, 'Status'),
                      h('th', { style: s.th }, 'Capabilities'),
                      h('th', { style: s.th }, 'Load'),
                      h('th', { style: s.th }, 'Heartbeat'),
                    )),
                    h('tbody', null,
                      state.nodes.map(function (node) {
                        var hb = node.last_heartbeat
                          ? (utils.isoTimeAgo ? utils.isoTimeAgo(node.last_heartbeat) : new Date(node.last_heartbeat).toLocaleString())
                          : '—';
                        return h('tr', { key: node.id },
                          h('td', { style: s.td },
                            h('div', null, node.name || node.id),
                            h('div', { style: { fontSize: '0.65rem', opacity: 0.4 } }, node.id),
                          ),
                          h('td', { style: s.td }, Badge('node', node.status)),
                          h('td', { style: s.td },
                            h('span', { style: { display: 'flex', flexWrap: 'wrap' } },
                              (node.capabilities || []).map(function (c) { return h('span', { key: c, style: { padding: '1px 6px', margin: '1px 4px 1px 0', borderRadius: '3px', fontSize: '0.65rem', background: 'rgba(255,255,255,0.06)' } }, c); }),
                            ),
                          ),
                          h('td', { style: Object.assign({}, s.td, { fontSize: '0.75rem' }) },
                            node.load !== undefined && node.load !== null ? (node.load * 100).toFixed(0) + '%' : '—',
                          ),
                          h('td', { style: Object.assign({}, s.td, { fontSize: '0.7rem', opacity: 0.6 }) }, hb),
                        );
                      })
                    ),
                  ),
                ),
              )
            : null,

          // Tasks table
          state.tasks && Array.isArray(state.tasks) && state.tasks.length > 0
            ? h(comp.Card, null,
                h(comp.CardHeader, null, h(comp.CardTitle, { style: { fontSize: '0.9rem' } }, 'Tasks (', state.tasks.length, ')')),
                h(comp.CardContent, { style: { padding: 0, overflowX: 'auto' } },
                  h('table', { style: s.table },
                    h('thead', null, h('tr', null,
                      h('th', { style: s.th }, 'ID'),
                      h('th', { style: s.th }, 'Title'),
                      h('th', { style: s.th }, 'Status'),
                      h('th', { style: s.th }, 'Assigned'),
                      h('th', { style: s.th }, 'Requires'),
                      h('th', { style: s.th }, 'Dependencies'),
                    )),
                    h('tbody', null,
                      state.tasks.map(function (task) {
                        return h('tr', { key: task.id },
                          h('td', { style: Object.assign({}, s.td, { fontFamily: 'monospace', fontSize: '0.7rem' }) }, task.id),
                          h('td', { style: s.td }, task.title || '—'),
                          h('td', { style: s.td }, Badge('task', task.status)),
                          h('td', { style: Object.assign({}, s.td, { fontSize: '0.75rem' }) }, task.assigned_to || '—'),
                          h('td', { style: s.td },
                            h('span', { style: { display: 'flex', flexWrap: 'wrap' } },
                              (task.requires || []).map(function (r) { return h('span', { key: r, style: { padding: '1px 6px', margin: '1px 4px 1px 0', borderRadius: '3px', fontSize: '0.65rem', background: 'rgba(255,255,255,0.06)' } }, r); })
                            ),
                          ),
                          h('td', { style: Object.assign({}, s.td, { fontSize: '0.7rem', opacity: 0.7 }) },
                            task.depends_on && task.depends_on.length > 0 ? task.depends_on.join(', ') : '—'
                          ),
                        );
                      })
                    ),
                  ),
                ),
              )
            : null,

          // Leases table
          state.leases && Array.isArray(state.leases) && state.leases.length > 0
            ? h(comp.Card, null,
                h(comp.CardHeader, null, h(comp.CardTitle, { style: { fontSize: '0.9rem' } }, 'Active Leases (', state.leases.length, ')')),
                h(comp.CardContent, { style: { padding: 0, overflowX: 'auto' } },
                  h('table', { style: s.table },
                    h('thead', null, h('tr', null,
                      h('th', { style: s.th }, 'Task ID'),
                      h('th', { style: s.th }, 'Node'),
                      h('th', { style: s.th }, 'Expires'),
                      h('th', { style: s.th }, 'Status'),
                    )),
                    h('tbody', null,
                      state.leases.map(function (l) {
                        return h('tr', { key: l.id },
                          h('td', { style: Object.assign({}, s.td, { fontFamily: 'monospace', fontSize: '0.7rem' }) }, l.task_id || l.id),
                          h('td', { style: Object.assign({}, s.td, { fontSize: '0.75rem' }) }, l.node_id || l.owner_node || '—'),
                          h('td', { style: Object.assign({}, s.td, { fontSize: '0.7rem', opacity: 0.6 }) }, l.lease_until || l.expires_at || '—'),
                          h('td', { style: s.td }, Badge('task', l.status || 'active')),
                        );
                      })
                    ),
                  ),
                ),
              )
            : null,
        ) : null,
      ),
    );
  }

  // -----------------------------------------------------------------------
  // Register
  // -----------------------------------------------------------------------

  window.__HERMES_PLUGINS__.register('agent-cluster', ClusterDashboard);
  console.log('[agent-cluster] Dashboard plugin registered');
})();
