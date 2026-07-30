# Design - Smart Storefront

A locked design system for this commerce app. Every production page reads this
file before visual changes are made. Extend the system here before adding local
exceptions.

Studied-DNA source: Bluebella's public storefront, reviewed 2026-07-27. The
system borrows only its dense retail rhythm, image-led hierarchy, and immediate
catalogue handoff. Brand, copy, imagery, layout details, and implementation are
original to this project.

## Genre

Editorial retail. Premium image direction with fast catalogue access and quiet
conversion controls.

## Macrostructure family

- Marketing and home: Photographic H6 with a compact lower-left message.
- Collection and search: Catalogue with F6 uniform product cards.
- Product detail: Split Diptych with image proof opposite purchase controls.
- Transaction and account: functional Workbench; forms and order data are the work surface.
- Content and policy: Long Document with a 65ch reading measure.

## Theme

Gallery Nocturne.

- `--color-paper` oklch(98.5% 0.004 70)
- `--color-paper-2` oklch(95.5% 0.006 70)
- `--color-ink` oklch(13% 0.008 30)
- `--color-ink-2` oklch(24% 0.01 30)
- `--color-rule` oklch(86% 0.006 70)
- `--color-accent` oklch(40% 0.13 20)
- `--color-focus` oklch(58% 0.19 255)

Near-black is the primary action color. Oxblood stays below 3 percent of each
viewport and marks focus, active states, and small editorial details. Product
photography supplies the wider palette.

## Typography

- Display: Bodoni Moda, weight 400, roman.
- Body: Manrope, weights 400 and 700.
- Outlier: system monospace, order numbers and compact data only.
- Display tracking: 0.
- Type scale: major third; home display capped at 3.815rem.
- Headings never use italic emphasis.

## Spacing

The named 4-point scale in `tokens.css` is mandatory. Page CSS uses
`var(--space-*)`; raw spacing values are reserved for intrinsic control geometry.

## Motion

Motion for React is the shared animation runtime.

- Hero: one orchestrated image and copy entrance.
- Catalogue: one-shot stagger for the first visible product shelf only.
- Commerce feedback: cart count, line insertion, and mobile purchase bar state.
- Easings: `--ease-out`, `--ease-in`, and `--ease-in-out`.
- Reduced motion: opacity-only at 150ms or less.
- No parallax, cursor followers, infinite decorative loops, or universal scroll reveals.

## Microinteractions stance

- Success is silent when the result is visible.
- Buttons press by 1px and return without bounce.
- Focus rings appear instantly.
- Hover is paired with focus and a coarse-pointer equivalent.
- Form errors state what happened and how to fix it.

## CTA voice

- Primary: near-black fill, near-square 2px radius, short verb-first label.
- Secondary: typographic link with a rule that thickens on hover.
- Inputs and buttons share a 48px minimum height.

## Per-page allowances

- Marketing pages may use CMS and product photography.
- Collection pages use uniform product imagery and no decorative cards.
- Transaction and account pages use no enrichment; the function carries the page.
- Content pages use typography only.

## What pages MUST share

- The store wordmark and type pairing.
- The white, black, and restrained oxblood palette.
- The rectangular CTA voice.
- Stacked, unnumbered section headings.
- N12 announcement plus retracting navigation.
- Ft1 mast-headed footer.

## What pages MAY differ on

- Image ratio according to page purpose.
- Product density across catalogue breakpoints.
- Sticky purchase controls on product and checkout pages.
- Motion timing within the three approved primitives.

## Exports

### tokens.css

```css
:root {
  --color-paper: oklch(98.5% 0.004 70);
  --color-paper-2: oklch(95.5% 0.006 70);
  --color-paper-3: oklch(91% 0.008 70);
  --color-rule: oklch(86% 0.006 70);
  --color-rule-2: oklch(70% 0.01 60);
  --color-muted: oklch(48% 0.012 45);
  --color-neutral: oklch(34% 0.01 35);
  --color-ink-2: oklch(24% 0.01 30);
  --color-ink: oklch(13% 0.008 30);
  --color-accent: oklch(40% 0.13 20);
  --color-accent-ink: oklch(98.5% 0.004 70);
  --color-focus: oklch(58% 0.19 255);

  --font-display: "Bodoni Moda", "Didot", ui-serif, serif;
  --font-body: "Manrope", "Avenir Next", ui-sans-serif, sans-serif;
  --font-outlier: ui-monospace, "SFMono-Regular", monospace;

  --space-3xs: 0.25rem;
  --space-2xs: 0.5rem;
  --space-xs: 0.75rem;
  --space-sm: 1rem;
  --space-md: 1.5rem;
  --space-lg: 2rem;
  --space-xl: 3rem;
  --space-2xl: 4rem;
  --space-3xl: 6rem;
  --space-4xl: 9rem;

  --text-xs: 0.75rem;
  --text-sm: 0.875rem;
  --text-base: 1rem;
  --text-md: 1.25rem;
  --text-lg: 1.5625rem;
  --text-xl: 1.953rem;
  --text-2xl: 2.441rem;
  --text-display: 3.815rem;

  --ease-out: cubic-bezier(0.16, 1, 0.3, 1);
  --ease-in: cubic-bezier(0.7, 0, 0.84, 0);
  --ease-in-out: cubic-bezier(0.65, 0, 0.35, 1);
  --dur-micro: 120ms;
  --dur-short: 240ms;
  --dur-long: 520ms;

  --rule-hair: 1px;
  --rule-fine: 2px;
  --radius-card: 0;
  --radius-pill: 999px;
  --radius-input: 2px;
}
```

### Tailwind v4 `@theme`

```css
@theme {
  --color-paper: oklch(98.5% 0.004 70);
  --color-paper-2: oklch(95.5% 0.006 70);
  --color-paper-3: oklch(91% 0.008 70);
  --color-rule: oklch(86% 0.006 70);
  --color-ink: oklch(13% 0.008 30);
  --color-accent: oklch(40% 0.13 20);
  --font-display: "Bodoni Moda", ui-serif, serif;
  --font-body: "Manrope", ui-sans-serif, sans-serif;
  --spacing-sm: 1rem;
  --spacing-md: 1.5rem;
  --spacing-lg: 2rem;
  --spacing-xl: 3rem;
  --text-md: 1.25rem;
  --text-xl: 1.953rem;
  --ease-out: cubic-bezier(0.16, 1, 0.3, 1);
}
```

### DTCG `tokens.json`

```json
{
  "$schema": "https://design-tokens.github.io/community-group/format/",
  "color": {
    "paper": { "$value": "oklch(98.5% 0.004 70)", "$type": "color" },
    "paper-2": { "$value": "oklch(95.5% 0.006 70)", "$type": "color" },
    "ink": { "$value": "oklch(13% 0.008 30)", "$type": "color" },
    "accent": { "$value": "oklch(40% 0.13 20)", "$type": "color" },
    "focus": { "$value": "oklch(58% 0.19 255)", "$type": "color" }
  },
  "font": {
    "display": { "$value": "Bodoni Moda, ui-serif, serif", "$type": "fontFamily" },
    "body": { "$value": "Manrope, ui-sans-serif, sans-serif", "$type": "fontFamily" }
  },
  "space": {
    "sm": { "$value": "1rem", "$type": "dimension" },
    "md": { "$value": "1.5rem", "$type": "dimension" },
    "lg": { "$value": "2rem", "$type": "dimension" },
    "xl": { "$value": "3rem", "$type": "dimension" }
  },
  "duration": {
    "micro": { "$value": "120ms", "$type": "duration" },
    "short": { "$value": "240ms", "$type": "duration" },
    "long": { "$value": "520ms", "$type": "duration" }
  }
}
```

### shadcn/ui CSS variables

```css
:root {
  --background: 98.5% 0.004 70;
  --foreground: 13% 0.008 30;
  --card: 95.5% 0.006 70;
  --card-foreground: 13% 0.008 30;
  --popover: 95.5% 0.006 70;
  --popover-foreground: 13% 0.008 30;
  --primary: 13% 0.008 30;
  --primary-foreground: 98.5% 0.004 70;
  --secondary: 91% 0.008 70;
  --secondary-foreground: 24% 0.01 30;
  --muted: 86% 0.006 70;
  --muted-foreground: 48% 0.012 45;
  --accent: 40% 0.13 20;
  --accent-foreground: 98.5% 0.004 70;
  --destructive: 50% 0.19 25;
  --destructive-foreground: 98.5% 0.004 70;
  --border: 86% 0.006 70;
  --input: 86% 0.006 70;
  --ring: 58% 0.19 255;
  --radius: 2px;
}
```
