import type { Inbound } from './types.js';

// This deliberately drops excess or stale acknowledgements rather than replaying
// them later. The durable inbox and callback delivery do not depend on this path.
const MAX_AGE_MS = 15_000;
const MAX_PENDING = 4;

export function personalGetReaction(
  receive: (message: Inbound) => { state: string },
  send: (appId: string, messageId: string) => Promise<void>,
  isOwner: (message: Pick<Inbound, 'appId' | 'tenantKey' | 'openId' | 'chatId'>) => boolean,
  now = Date.now,
) {
  let pending = 0;
  let stopped = false;
  const activatedAt = now();
  return {
    // Used only behind the authenticated WebSocket adapter, which validates
    // sender type, private chat, and message decoding before calling receive.
    receive(message: Inbound) {
      const result = receive(message);
      if (stopped || result.state !== 'accepted' || message.contentStatus || message.richPost ||
          message.text.startsWith('/bind') || pending >= MAX_PENDING) return result;
      const sentAt = Date.parse(message.timestamp);
      const fresh = () => Number.isFinite(sentAt) && sentAt >= activatedAt &&
        sentAt >= now() - MAX_AGE_MS && sentAt <= now() + 5_000;
      if (!fresh()) return result;
      const { appId, tenantKey, openId, chatId, messageId } = message;
      pending++;
      setImmediate(async () => {
        try {
          if (!stopped && fresh() && isOwner({ appId, tenantKey, openId, chatId })) await send(appId, messageId);
        } catch {
          // No retries; errors cannot change inbound storage, ACK, or callback.
          console.warn('{"event":"feishu_get_reaction_failed"}');
        } finally { pending--; }
      });
      return result;
    },
    stop() { stopped = true; },
  };
}
